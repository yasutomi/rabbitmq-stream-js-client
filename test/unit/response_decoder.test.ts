import { expect } from "chai"
import { amqpEncode } from "../../src/amqp10/encoder"
import { NoneCompression } from "../../src/compression"
import { DecoderListenerFunc } from "../../src/decoder_listener"
import { ResponseDecoder } from "../../src/response_decoder"
import { DeliverResponse } from "../../src/responses/deliver_response"
import { PeerPropertiesResponse } from "../../src/responses/peer_properties_response"
import { Response } from "../../src/responses/response"
import { createConsoleLog } from "../support/util"
import { BufferDataWriter } from "../../src/requests/buffer_data_writer"

class MockDecoderListener {
  readonly responses: Response[] = []

  reset() {
    this.responses.splice(0)
  }

  responseReceived(data: Response) {
    this.responses.push(data)
  }

  buildListener(): DecoderListenerFunc {
    this.reset()
    return (...args) => this.responseReceived(...args)
  }
}

describe("ResponseDecoder", () => {
  let decoder: ResponseDecoder
  const mockListener = new MockDecoderListener()
  const getCompressionBy = () => NoneCompression.create()

  beforeEach(() => {
    decoder = new ResponseDecoder(mockListener.buildListener(), createConsoleLog())
  })

  it("decode a buffer that contains a single response", () => {
    const data = createResponse({ key: PeerPropertiesResponse.key })

    decoder.add(data, getCompressionBy)

    expect(mockListener.responses).lengthOf(1)
  })

  it("decode a buffer that contains multiple responses", () => {
    const data = [
      createResponse({ key: PeerPropertiesResponse.key }),
      createResponse({ key: PeerPropertiesResponse.key }),
    ]

    decoder.add(Buffer.concat(data), getCompressionBy)

    expect(mockListener.responses).lengthOf(2)
  })

  it("keeps one chunk timestamp with every message decoded from a Deliver chunk", () => {
    const chunkTimestampMs = 1_727_654_321_000
    let delivery: DeliverResponse | undefined
    decoder.on("deliverV1", (response) => {
      delivery = response
    })

    decoder.add(createDeliverResponse(chunkTimestampMs, [Buffer.from("one"), Buffer.from("two")]), getCompressionBy)

    expect(delivery).not.undefined
    const response = delivery!
    expect(response.chunkTimestampMs).eql(chunkTimestampMs)
    expect(response.messages.map((message) => message.content)).eql([Buffer.from("one"), Buffer.from("two")])
    expect(response.messages.map((message) => message.offset)).eql([42n, 43n])
  })
})

function createResponse(params: { key: number; correlationId?: number; responseCode?: number }): Buffer {
  const bufferSize = 1024
  const bufferSizeParams = { maxSize: bufferSize }
  const dataWriter = new BufferDataWriter(Buffer.alloc(bufferSize), 4, bufferSizeParams)
  dataWriter.writeUInt16(params.key)
  dataWriter.writeUInt16(1)
  dataWriter.writeUInt32(params.correlationId || 101)
  dataWriter.writeUInt16(params.responseCode || 1)

  switch (params.key) {
    case PeerPropertiesResponse.key:
      dataWriter.writeInt32(0)
      break

    default:
      break
  }

  dataWriter.writePrefixSize()
  return dataWriter.toBuffer()
}

function createDeliverResponse(chunkTimestampMs: number, contents: Buffer[]): Buffer {
  const bufferSizeParams = { maxSize: 1024 }
  const dataWriter = new BufferDataWriter(Buffer.alloc(1024), 4, bufferSizeParams)
  dataWriter.writeUInt16(DeliverResponse.key)
  dataWriter.writeUInt16(DeliverResponse.Version)
  dataWriter.writeUInt8(1)
  dataWriter.writeInt8(1)
  dataWriter.writeInt8(0)
  dataWriter.writeUInt16(contents.length)
  dataWriter.writeUInt32(contents.length)
  dataWriter.writeInt64(BigInt(chunkTimestampMs))
  dataWriter.writeUInt64(1n)
  dataWriter.writeUInt64(42n)
  dataWriter.writeInt32(0)
  dataWriter.writeUInt32(0)
  dataWriter.writeUInt32(0)
  dataWriter.writeUInt32(0)
  for (const content of contents) amqpEncode(dataWriter, { content })
  dataWriter.writePrefixSize()
  return dataWriter.toBuffer()
}
