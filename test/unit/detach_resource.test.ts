import { expect } from "chai"
import { Client } from "../../src"
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

    await client.detachPublisher(p1)
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
