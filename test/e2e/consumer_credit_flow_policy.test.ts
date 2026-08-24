import { use as chaiUse, expect, spy } from "chai"
import { Client, Publisher } from "../../src"
import { Message } from "../../src/publisher"
import { Offset } from "../../src/requests/subscribe_request"
import { createClient, createConsumerRef, createPublisher, createStreamName } from "../support/fake_data"
import { Rabbit } from "../support/rabbit"
import { always, eventually, mapSync, password, username, waitSleeping } from "../support/util"
import {
  ConsumerChunkCreditController,
  CreditRequestWrapper,
  creditsOnChunkCompleted,
  creditsOnChunkReceived,
} from "../../src/consumer_credit_policy"
import spies from "chai-spies"
chaiUse(spies)

const send = async (publisher: Publisher, chunk: Message[]) => {
  await mapSync(chunk, (m) => publisher.send(m.content))
  await publisher.flush()
}

describe("consumer credit flow policies", () => {
  let streamName: string
  let invocationTimestamp: undefined | number = undefined
  let lowerBoundTimestamp: undefined | number = undefined
  let upperBoundTimestamp: undefined | number = undefined
  const rabbit = new Rabbit(username, password)
  let client: Client
  let publisher: Publisher
  const previousMaxSharedClientInstances = process.env.MAX_SHARED_CLIENT_INSTANCES
  const sandbox = spy.sandbox()
  const chunk: Message[] = [
    { content: Buffer.from("hello") },
    { content: Buffer.from("there") },
    { content: Buffer.from("brave") },
    { content: Buffer.from("new") },
    { content: Buffer.from("world") },
  ]
  const nMessages = chunk.length
  const received: Message[] = []

  const messageHandler = (lowerBoundThreshold: number, upperBoundThreshold: number) => async (message: Message) => {
    received.push(message)
    if (received.length % chunk.length === lowerBoundThreshold) {
      lowerBoundTimestamp = Date.now()
      await waitSleeping(10)
    }
    if (received.length % chunk.length === upperBoundThreshold) {
      upperBoundTimestamp = Date.now()
      await waitSleeping(10)
    }
  }

  const requestCreditsWrapper = async (_requestWrapper: CreditRequestWrapper, _amount: number) => {
    invocationTimestamp = Date.now()
  }

  before(() => {
    process.env.MAX_SHARED_CLIENT_INSTANCES = "10"
  })

  after(() => {
    if (previousMaxSharedClientInstances !== undefined) {
      process.env.MAX_SHARED_CLIENT_INSTANCES = previousMaxSharedClientInstances
      return
    }
    delete process.env.MAX_SHARED_CLIENT_INSTANCES
  })

  beforeEach(async () => {
    client = await createClient(username, password)
    streamName = createStreamName()
    await rabbit.createStream(streamName)
    publisher = await createPublisher(streamName, client)
    invocationTimestamp = undefined
    lowerBoundTimestamp = undefined
    upperBoundTimestamp = undefined
    received.splice(0, received.length)
  })

  afterEach(async () => {
    try {
      sandbox.restore()
      await client.close()
      await rabbit.deleteStream(streamName)
      await rabbit.closeAllConnections()
      await rabbit.deleteAllQueues({ match: /my-stream-/ })
    } catch (_e) {}
  })

  it("NewCreditOnChunkReceived policy requests new credit when chunk is completely received", async () => {
    const consumerRef = createConsumerRef()
    const policy = creditsOnChunkReceived(1, 1)
    sandbox.on(policy, "requestCredits", requestCreditsWrapper)
    sandbox.on(policy, "onChunkReceived")
    await client.declareConsumer(
      {
        stream: streamName,
        offset: Offset.first(),
        singleActive: true,
        consumerRef,
        creditPolicy: policy,
      },
      messageHandler(0, 1)
    )

    await send(publisher, chunk)

    await eventually(() => expect(received.length).eql(nMessages))
    await eventually(() => expect(policy.requestCredits).called.exactly(1))
    await eventually(() => expect(policy.onChunkReceived).called.exactly(1))
    await always(() => expect(policy.onChunkReceived).called.below(1 + 1), 5000)
    expect(invocationTimestamp).lessThanOrEqual(upperBoundTimestamp!)
  }).timeout(10000)

  it("NewCreditOnChunkCompleted policy requests new credit when chunk completely handled", async () => {
    const consumerRef = createConsumerRef()
    const policy = creditsOnChunkCompleted(1, 1)
    sandbox.on(policy, "requestCredits", requestCreditsWrapper)
    sandbox.on(policy, "onChunkCompleted")
    await client.declareConsumer(
      {
        stream: streamName,
        offset: Offset.first(),
        singleActive: true,
        consumerRef,
        creditPolicy: policy,
      },
      messageHandler(1, chunk.length)
    )

    await send(publisher, chunk)

    await eventually(() => expect(received.length).eql(nMessages))
    await eventually(() => expect(policy.onChunkCompleted).called.exactly(1))
    await eventually(() => expect(policy.requestCredits).called.exactly(1))
    await always(() => expect(policy.onChunkCompleted).called.below(1 + 1), 5000)
    expect(invocationTimestamp).greaterThanOrEqual(lowerBoundTimestamp!)
  }).timeout(10000)

  it("opt-in controller requests exactly one next credit after a handled chunk", async () => {
    const requested: number[] = []
    const controller: ConsumerChunkCreditController = {
      initialCredit: 1,
      shouldIssueNextCredit: async (context) => {
        requested.push(context.chunkId)
        return true
      },
    }
    await client.declareConsumer(
      { stream: streamName, offset: Offset.first(), chunkCreditController: controller },
      messageHandler(0, 1)
    )
    await send(publisher, chunk)

    await eventually(() => expect(received.length).eql(nMessages))
    await eventually(() => expect(requested).eql([0]))
    await always(() => expect(requested).eql([0]), 5000)
  }).timeout(10000)

  for (const invalidInitialCredit of [0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY]) {
    it(`rejects invalid initial credit ${invalidInitialCredit}`, async () => {
      const controller: ConsumerChunkCreditController = {
        initialCredit: invalidInitialCredit,
        shouldIssueNextCredit: async () => true,
      }
      await expect(
        client.declareConsumer(
          { stream: streamName, offset: Offset.first(), chunkCreditController: controller },
          async () => undefined
        )
      ).to.be.rejectedWith("chunkCreditController.initialCredit must be a positive integer")
    })
  }

  it("does not use an old hook completion to issue credit after restart", async () => {
    let resolvePermit!: (value: boolean) => void
    const permit = new Promise<boolean>((resolve) => {
      resolvePermit = resolve
    })
    const generations: number[] = []
    await client.declareConsumer(
      {
        stream: streamName,
        offset: Offset.first(),
        chunkCreditController: {
          initialCredit: 1,
          shouldIssueNextCredit: async (context) => {
            generations.push(context.generation)
            return permit
          },
        },
      },
      async (message) => {
        received.push(message)
      }
    )
    await send(publisher, [{ content: Buffer.from("before-restart") }])
    await eventually(() => expect(generations).eql([0]))
    await eventually(() => expect(received).to.have.length(1))
    await send(publisher, [{ content: Buffer.from("waiting") }])

    const restarting = client.restart()
    resolvePermit(true)
    await always(() => expect(received).to.have.length(1), 4000)
    await restarting
    await eventually(() => expect(received.length).to.be.greaterThanOrEqual(2))
    await eventually(() => {
      expect(generations[0]).eql(0)
      expect(generations.slice(1)).to.satisfy(
        (values: number[]) => values.length >= 1 && values.every((value) => value === 1)
      )
    })
  }).timeout(20000)
})
