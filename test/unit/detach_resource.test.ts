import { expect } from "chai"
import { Client } from "../../src"
import type { Consumer } from "../../src/consumer"
import type { Publisher } from "../../src/publisher"

describe("Client detachPublisher", () => {
  it("does not detach a replacement that reuses the old extended ID", async () => {
    const extendedId = "1@connection"
    const unregister = { calls: 0 }
    const p1 = closeablePublisher(extendedId)
    const p2 = closeablePublisher(extendedId)
    const client = Object.create(Client.prototype) as unknown as {
      detachPublisher(publisher: Publisher): Promise<void>
      publishers: Map<string, { publisher: Publisher; connection: { unregisterForClosePublisher(id: string): void } }>
    }
    client.publishers = new Map([
      [extendedId, { publisher: p1, connection: { unregisterForClosePublisher: () => unregister.calls++ } }],
    ])

    await Promise.all([client.detachPublisher(p1), client.detachPublisher(p1)])
    client.publishers.set(extendedId, {
      publisher: p2,
      connection: { unregisterForClosePublisher: () => unregister.calls++ },
    })
    await client.detachPublisher(p1)

    expect(client.publishers.get(extendedId)?.publisher).to.equal(p2)
    expect(unregister.calls).to.equal(1)
    expect(p1.closeCalls()).to.equal(1)
  })
})

describe("Client detachConsumer", () => {
  it("removes ownership and callback before a rejected local close", async () => {
    const extendedId = "1@connection"
    const failure = new Error("release failed")
    let unregisters = 0
    const consumer = { extendedId, close: () => Promise.reject(failure) } as unknown as Consumer
    const client = Object.create(Client.prototype) as unknown as {
      chunkCreditStates: Map<string, unknown>
      consumers: Map<string, { consumer: Consumer; connection: { unregisterForCloseConsumer(id: string): void } }>
      detachConsumer(consumer: Consumer): Promise<void>
    }
    client.chunkCreditStates = new Map([[extendedId, {}]])
    client.consumers = new Map([
      [extendedId, { consumer, connection: { unregisterForCloseConsumer: () => unregisters++ } }],
    ])

    await expect(client.detachConsumer(consumer)).to.be.rejectedWith(failure)
    expect(client.consumers.has(extendedId)).to.equal(false)
    expect(client.chunkCreditStates.has(extendedId)).to.equal(false)
    expect(unregisters).to.equal(1)
  })
})

describe("Client close", () => {
  it("detaches every resource and aggregates cleanup failures", async () => {
    const p = failingPublisher("1@connection", new Error("publisher close"))
    const c = {
      extendedId: "2@connection",
      close: () => Promise.reject(new Error("consumer close")),
    } as unknown as Consumer
    const client = Object.create(Client.prototype) as unknown as {
      chunkCreditStates: Map<string, unknown>
      consumers: Map<string, { consumer: Consumer; connection: { unregisterForCloseConsumer(id: string): void } }>
      publishers: Map<string, { publisher: Publisher; connection: { unregisterForClosePublisher(id: string): void } }>
      close(): Promise<void>
      locatorConnection: { close(): Promise<void> }
      logger: { info(message: string): void }
    }
    client.chunkCreditStates = new Map()
    client.publishers = new Map([
      [p.extendedId, { publisher: p, connection: { unregisterForClosePublisher: () => undefined } }],
    ])
    client.consumers = new Map([
      [c.extendedId, { consumer: c, connection: { unregisterForCloseConsumer: () => undefined } }],
    ])
    client.locatorConnection = { close: () => Promise.reject(new Error("locator close")) }
    client.logger = { info: () => undefined }

    await expect(client.close()).to.be.rejectedWith(AggregateError)
    expect(client.publishers.size).to.equal(0)
    expect(client.consumers.size).to.equal(0)
  })
})

describe("Client restart", () => {
  it("does not redeclare a publisher detached while its connection restart is pending", async () => {
    const publisher = closeablePublisher("1@publisher-connection")
    const connectionRestart = deferred<void>()
    let connectionRestartStarted = false
    let redeclarations = 0
    const client = Object.create(Client.prototype) as unknown as {
      chunkCreditStates: Map<string, unknown>
      consumers: Map<string, unknown>
      publishers: Map<
        string,
        {
          publisher: Publisher
          connection: { connectionId: string; restart(): Promise<void>; unregisterForClosePublisher(id: string): void }
          params: unknown
          filter: unknown
        }
      >
      locatorConnection: { connectionId: string; restart(): Promise<void> }
      logger: { info(message: string): void }
      detachPublisher(publisher: Publisher): Promise<void>
      restart(): Promise<void>
      declarePublisherOnConnection(): Promise<void>
    }
    client.chunkCreditStates = new Map()
    client.consumers = new Map()
    client.locatorConnection = { connectionId: "locator", restart: async () => undefined }
    client.logger = { info: () => undefined }
    client.declarePublisherOnConnection = async () => {
      redeclarations += 1
    }
    client.publishers = new Map([
      [
        publisher.extendedId,
        {
          publisher,
          connection: {
            connectionId: "publisher-connection",
            restart: () => {
              connectionRestartStarted = true
              return connectionRestart.promise
            },
            unregisterForClosePublisher: () => undefined,
          },
          params: {},
          filter: undefined,
        },
      ],
    ])

    const restarting = client.restart()
    await eventually(() => connectionRestartStarted)
    await client.detachPublisher(publisher)
    connectionRestart.resolve()
    await restarting

    expect(redeclarations).to.equal(0)
  }).timeout(10000)

  it("does not redeclare a consumer detached while its connection restart is pending", async () => {
    const consumer = {
      close: async () => undefined,
      extendedId: "2@consumer-connection",
      getOffset: () => 0n,
    } as unknown as Consumer
    const connectionRestart = deferred<void>()
    let connectionRestartStarted = false
    let redeclarations = 0
    const client = Object.create(Client.prototype) as unknown as {
      chunkCreditStates: Map<string, unknown>
      consumers: Map<
        string,
        {
          consumer: Consumer
          connection: { connectionId: string; restart(): Promise<void>; unregisterForCloseConsumer(id: string): void }
          params: unknown
        }
      >
      publishers: Map<string, unknown>
      locatorConnection: { connectionId: string; restart(): Promise<void> }
      logger: { info(message: string): void }
      declareConsumerOnConnection(): Promise<void>
      detachConsumer(consumer: Consumer): Promise<void>
      restart(): Promise<void>
    }
    client.chunkCreditStates = new Map()
    client.publishers = new Map()
    client.locatorConnection = { connectionId: "locator", restart: async () => undefined }
    client.logger = { info: () => undefined }
    client.declareConsumerOnConnection = async () => {
      redeclarations += 1
    }
    client.consumers = new Map([
      [
        consumer.extendedId,
        {
          consumer,
          connection: {
            connectionId: "consumer-connection",
            restart: () => {
              connectionRestartStarted = true
              return connectionRestart.promise
            },
            unregisterForCloseConsumer: () => undefined,
          },
          params: {},
        },
      ],
    ])

    const restarting = client.restart()
    await eventually(() => connectionRestartStarted)
    await client.detachConsumer(consumer)
    connectionRestart.resolve()
    await restarting

    expect(redeclarations).to.equal(0)
  }).timeout(10000)
})

function closeablePublisher(extendedId: string): Publisher & { closeCalls(): number } {
  let calls = 0
  let closePromise: Promise<void> | undefined
  return {
    basicSend: async () => ({ connectionId: "connection", publisherId: 1, publishingId: 1n, sent: true }),
    close: () =>
      (closePromise ??= Promise.resolve().then(() => {
        calls += 1
      })),
    closed: false,
    extendedId,
    getConnectionInfo: () => ({ host: "localhost", id: "connection", port: 5552, ready: true, vhost: "/" }),
    getLastPublishingId: async () => 0n,
    on: () => undefined,
    publisherId: 1,
    ref: "publisher",
    send: async () => ({ connectionId: "connection", publisherId: 1, publishingId: 1n, sent: true }),
    flush: async () => true,
    sendSubEntries: async () => undefined,
    closeCalls: () => calls,
  }
}

function failingPublisher(extendedId: string, failure: Error): Publisher {
  return { ...closeablePublisher(extendedId), close: () => Promise.reject(failure) }
}

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void
  const promise = new Promise<T>((complete) => {
    resolve = complete
  })
  return { promise, resolve }
}

async function eventually(predicate: () => boolean): Promise<void> {
  for (let attempts = 0; attempts < 60; attempts += 1) {
    if (predicate()) return
    await new Promise((resolve) => setTimeout(resolve, 100))
  }
  throw new Error("Timed out waiting for condition")
}
