import { expect } from "chai"
import { Client, Offset } from "../../src"
import { createClient, createStreamName } from "../support/fake_data"
import { Rabbit } from "../support/rabbit"
import { eventually, username, password } from "../support/util"
import { Consumer } from "../../src/consumer"
import { Message, Publisher } from "../../src/publisher"
import { randomUUID } from "crypto"

describe("restart connections", () => {
  const rabbit = new Rabbit(username, password)
  let streamName: string
  let client: Client

  beforeEach(async () => {
    client = await createClient(username, password)
    streamName = createStreamName()
    await rabbit.createStream(streamName)
  })

  afterEach(async () => {
    try {
      await client.close()
      await rabbit.deleteStream(streamName)
      await rabbit.closeAllConnections()
      await rabbit.deleteAllQueues({ match: /my-stream-/ })
    } catch (e) {}
  })

  it("client only, no producer or consumer, socket connection is reestablished", async () => {
    const oldConnectionInfo = client.getConnectionInfo()

    await client.restart()

    await eventually(async () => {
      const connectionInfo = client.getConnectionInfo()
      expect(connectionInfo.ready).eql(true)
      expect(connectionInfo.localPort).not.undefined
      expect(oldConnectionInfo.localPort).not.eql(connectionInfo.localPort)
    })
  }).timeout(10000)

  it("client with publishers and consumers, socket connections are reestablished", async () => {
    const clientOldConnectionInfo = client.getConnectionInfo()
    const streamNames = [streamName, createStreamName(), createStreamName()]
    const publishers = new Map<number, Publisher>()
    const localPublisherPorts = new Map<number, number>()
    const localConsumerPorts = new Map<number, number>()
    const consumers = new Map<number, Consumer>()
    const dummyMsgHandler = (_msg: Message) => {
      return
    }
    for (const stream of streamNames) {
      await rabbit.createStream(stream)
      const publisher = await client.declarePublisher({ stream: stream })
      const consumer1 = await client.declareConsumer({ stream: stream, offset: Offset.first() }, dummyMsgHandler)
      const consumer2 = await client.declareConsumer({ stream: stream, offset: Offset.first() }, dummyMsgHandler)
      publishers.set(publisher.publisherId, publisher)
      consumers.set(consumer1.consumerId, consumer1)
      consumers.set(consumer2.consumerId, consumer2)
      localPublisherPorts.set(publisher.publisherId, publisher.getConnectionInfo().localPort!)
      localConsumerPorts.set(consumer1.consumerId, consumer1.getConnectionInfo().localPort!)
      localConsumerPorts.set(consumer2.consumerId, consumer2.getConnectionInfo().localPort!)
    }

    await client.restart()

    await eventually(async () => {
      const clientConnectionInfo = client.getConnectionInfo()
      expect(clientConnectionInfo.localPort).is.not.undefined
      expect(clientConnectionInfo.localPort).not.eql(clientOldConnectionInfo.localPort)
      expect(clientConnectionInfo.ready).eql(true)
      for (const consumerId of consumers.keys()) {
        const consumer = consumers.get(consumerId)
        const consumerConnectionInfo = consumer!.getConnectionInfo()
        expect(consumerConnectionInfo.ready).eql(true)
        expect(consumerConnectionInfo.localPort).not.undefined
        expect(consumerConnectionInfo.localPort).not.eql(localConsumerPorts.get(consumerId))
      }
      for (const publisherId of publishers.keys()) {
        const publisher = publishers.get(publisherId)
        const publisherConnectionInfo = publisher!.getConnectionInfo()
        expect(publisherConnectionInfo.ready).eql(true)
        expect(publisherConnectionInfo.localPort).not.undefined
        expect(publisherConnectionInfo.localPort).not.eql(localPublisherPorts.get(publisherId))
      }
    }, 10000)
  }).timeout(20000)

  it("detached publishers and consumers are not redeclared during restart", async () => {
    const publisher = await client.declarePublisher({ stream: streamName })
    const consumer = await client.declareConsumer({ stream: streamName, offset: Offset.first() }, () => undefined)

    const restarting = client.restart()
    await client.detachPublisher(publisher)
    await client.detachConsumer(consumer)
    await restarting

    expect(client.publisherCounts()).eql(0)
    expect(client.consumerCounts()).eql(0)
  }).timeout(10000)

  it("does not redeclare detached resources after broker-triggered recovery", async () => {
    let recovery: Promise<void> | undefined
    await client.close()
    client = await createClient(username, password, {
      connection_closed: () => {
        recovery ??= client.restart()
      },
    })
    const publisherRef = `detached-publisher-${randomUUID()}`
    const consumerRef = `detached-consumer-${randomUUID()}`
    let detachedConsumerDeliveries = 0
    const publisher = await client.declarePublisher({ publisherRef, stream: streamName })
    const consumer = await client.declareConsumer(
      {
        connectionClosedListener: () => {
          recovery ??= client.restart()
        },
        consumerRef,
        offset: Offset.next(),
        stream: streamName,
      },
      () => {
        detachedConsumerDeliveries += 1
      }
    )
    await new Promise((resolve) => setTimeout(resolve, 5000))
    const consumerConnection = (await rabbit.getConsumers()).find(({ queue }) => queue.name === streamName)
    expect(consumerConnection).not.undefined

    await rabbit.closeStreamConnection(consumerConnection!.channel_details.connection_name)
    await eventually(() => expect(recovery).not.undefined)
    await client.detachPublisher(publisher)
    await client.detachConsumer(consumer)
    await recovery

    expect(client.publisherCounts()).eql(0)
    expect(client.consumerCounts()).eql(0)
    await expect(publisher.send(Buffer.from("detached"))).to.be.rejectedWith("closed")
    await eventually(async () => {
      expect(await rabbit.returnPublishers(streamName)).not.include(publisherRef)
      expect(await rabbit.returnConsumersIdentifiers()).not.include(consumerRef)
    })

    const replacementReceived = new Set<string>()
    await client.declareConsumer({ offset: Offset.next(), stream: streamName }, (message) => {
      replacementReceived.add(message.content.toString())
    })
    const replacementPublisher = await client.declarePublisher({ stream: streamName })
    await replacementPublisher.send(Buffer.from("replacement"))
    await replacementPublisher.flush()
    await eventually(() => {
      expect(detachedConsumerDeliveries).eql(0)
      expect(replacementReceived.has("replacement")).true
    })
  }).timeout(20000)

  it("sending and receiving messages is not affected", async () => {
    const received = new Set<string>()
    const messageNumber = 10000
    const triggerIndex = Math.floor(messageNumber / 4)
    const consumeHandle = (msg: Message) => {
      received.add(msg.messageProperties?.messageId!)
    }
    const oldClientConnectionInfo = client.getConnectionInfo()
    await client.declareConsumer({ stream: streamName, offset: Offset.first() }, consumeHandle)
    const publisher = await client.declarePublisher({ stream: streamName })

    for (let i = 0; i < messageNumber; i++) {
      const msg = Buffer.from(`${randomUUID()}`)
      await publisher.send(msg, { messageProperties: { messageId: `${i}` } })
      if (i === triggerIndex) await client.restart()
    }

    await eventually(async () => {
      const clientConnectionInfo = client.getConnectionInfo()
      expect(clientConnectionInfo.ready).eql(true)
      expect(clientConnectionInfo.localPort).not.eql(oldClientConnectionInfo.localPort)
      expect(received.size).eql(messageNumber)
    }, 10000)
  }).timeout(20000)
})
