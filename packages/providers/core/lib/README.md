# nv-provider-core

## Messaging

Messaging w/ nv provider core's streaming socket listener is defined below.
The listener maintains a full duplex connection with the client,
requests and responses may be sent/received in any order.

### Config Collection Protocol

The Config Collection Protocol defines the structure
of messages and behaviour of system components.

Tagged message w/ byte order:

```text
Message: [HEADER][PAYLOAD]
  HEADER: 8 bytes - [VERSION][OPCODE][CHECKSUM][PAYLOAD_LENGTH]
    VERSION: 1 byte
    OPCODE: 1 byte
    CHECKSUM: 2 bytes
    PAYLOAD_LENGTH: 4 bytes
  PAYLOAD: ...
```

#### Version

Majors only. e.g. 0x01, 0x02, 0x03

#### Opcodes

- INITIALIZE: 0x01
- DESTROY: 0x02
- GET_VALUE: 0x03
- HEALTH: 0x03

#### Checksum

16-bit ones' complement of the header and payload.
The checksum is initially set to 0 for checksum calculation.

1. Generate a header version, opcode, 0x0 (nil checksum), payload length
1. Group by 16 bit words
1. Pad 0's to complete words
1. Binary add all words
1. Wrap overflows
1. Ones complement

##### Error detection

1. Decode message as bytes
1. nil the checksum
1. Group by 16 bits
1. Pad 0's to complete
1. Binary add
1. Wrap overflows
1. Add checksum
1. All 1's, else error

> TODO: double check
