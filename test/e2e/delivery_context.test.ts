import { expect } from "chai"
import { Client, Offset, StreamDeliveryContext } from "../../src"
import { createClient, createPublisher, createStreamName } from "../support/fake_data"
import { Rabbit } from "../support/rabbit"
import { eventually, password, username } from "../support/util"

describe("delivery context", () => {
  const rabbit = new Rabbit(username, password)
  let client: Client
  let streamName: string

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
    } catch (_error) {}
  })

  it("keeps one chunk timestamp for every message and replay after restart", async () => {
    const publisher = await createPublisher(streamName, client)
    const initial: StreamDeliveryContext[] = []
    const replayed: StreamDeliveryContext[] = []

    await publisher.basicSend(1n, Buffer.from("one"))
    await publisher.basicSend(2n, Buffer.from("two"))
    await publisher.flush()
    await client.declareConsumer({ stream: streamName, offset: Offset.first() }, (_message, context) => {
      initial.push(context)
    })
    await eventually(() => expect(initial).lengthOf(2))

    expect(initial.map(({ offset }) => offset)).eql([0n, 1n])
    expect(initial.every(({ chunkTimestampMs }) => Number.isSafeInteger(chunkTimestampMs))).true
    const sourceContexts = [...initial]

    await client.restart()
    await client.declareConsumer({ stream: streamName, offset: Offset.first() }, (_message, context) => {
      replayed.push(context)
    })
    await eventually(() => expect(replayed).lengthOf(2))

    expect(replayed).eql(sourceContexts)
  }).timeout(10000)
})
