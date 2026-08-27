import { expect } from "chai"
import { ConsumerUpdateResponse } from "../../src/requests/consumer_update_response"
import { Offset } from "../../src/requests/subscribe_request"

describe("ConsumerUpdateResponse", () => {
  it("encodes an inactive response with OffsetType=0", () => {
    const bytes = new ConsumerUpdateResponse({
      correlationId: 1,
      responseCode: 1,
      offset: Offset.none(),
    }).toBuffer()

    expect(bytes.subarray(-2)).eql(Buffer.from([0, 0]))
  })

  it("encodes an active numeric response with OffsetType=4 and its UINT64", () => {
    const bytes = new ConsumerUpdateResponse({
      correlationId: 1,
      responseCode: 1,
      offset: Offset.offset(9n),
    }).toBuffer()

    expect(bytes.readUInt16BE(bytes.length - 10)).eql(4)
    expect(bytes.readBigUInt64BE(bytes.length - 8)).eql(9n)
  })
})
