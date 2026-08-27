import { expect } from "chai"
import { Client } from "../../src/client"
import { computeExtendedConsumerId } from "../../src/consumer"
import { Offset } from "../../src/requests/subscribe_request"
import { ConsumerUpdateQuery } from "../../src/responses/consumer_update_query"

describe("consumer update callback", () => {
  for (const active of [true, false]) {
    it(`passes active=${active} to the third listener argument`, async () => {
      const { callback, sent } = createCallback(async (_ref, _stream, context) => {
        expect(context.active).eql(active)
        return active ? Offset.offset(9n) : undefined
      })

      await callback(query(active))

      const bytes = sent.at(0)!.toBuffer()
      expect(bytes.subarray(-2)).eql(active ? Buffer.from([0, 9]) : Buffer.from([0, 0]))
    })
  }

  it("returns InternalError and OffsetType=0 when the listener fails", async () => {
    const { callback, sent } = createCallback(async () => {
      throw new Error("barrier failed")
    })

    await callback(query(true))

    const bytes = sent.at(0)!.toBuffer()
    expect(bytes.readUInt16BE(bytes.length - 4)).eql(15)
    expect(bytes.subarray(-2)).eql(Buffer.from([0, 0]))
  })
})

function createCallback(listener: (ref: string, stream: string, context: { active: boolean }) => Promise<Offset | undefined>) {
  const sent: { toBuffer(): Buffer }[] = []
  const client = Object.create(Client.prototype) as {
    consumers: Map<string, unknown>
    logger: { debug(): void; error(): void }
    getConsumerUpdateCallback(connectionId: string): (query: ConsumerUpdateQuery) => Promise<void>
  }
  client.logger = { debug: () => undefined, error: () => undefined }
  client.consumers = new Map([
    [
      computeExtendedConsumerId(1, "connection"),
      {
        consumer: {
          consumerRef: "ref",
          streamName: "stream",
          isSingleActive: true,
          consumerUpdateListener: listener,
          offset: Offset.first(),
          updateConsumerOffset: () => undefined,
        },
        connection: { send: async (response: { toBuffer(): Buffer }) => sent.push(response) },
      },
    ],
  ])
  return { callback: client.getConsumerUpdateCallback("connection"), sent }
}

function query(active: boolean): ConsumerUpdateQuery {
  return { active: active ? 1 : 0, correlationId: 1, subscriptionId: 1 } as ConsumerUpdateQuery
}
