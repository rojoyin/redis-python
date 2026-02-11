class RDBParser:
    code_metadata = b"\xfa"
    code_db_size = b"\xfb"
    code_db_start = b"\xfe"
    code_expiretime_ms = b"\xfc"
    code_expiretime_secs = b"\xfd"
    code_eof = b"\xff"

    def parse_file(self, rdb_file_path) -> dict[bytes, bytes]:
        if not rdb_file_path:
            return {}

        data = {}
        with open(rdb_file_path, "rb") as f:

            magic_string = f.read(9)

            if not magic_string.startswith(b"REDIS"):
                raise ValueError("Invalid RDB file: bad magic string")

            parsed_code = f.read(1)

            while parsed_code and parsed_code!=self.code_eof:

                if parsed_code == self.code_metadata:
                    key_length = _parse_length(f)

                    if key_length<0:
                        raise Exception("Encoding error")

                    metadata_key = f.read(key_length)
                    value_length = _parse_length(f)
                    metadata_value = f.read(value_length)
                    print(f"Metadata {metadata_key}: {metadata_value}")

                elif parsed_code == self.code_db_start:
                    db_index = _parse_length(f)
                    print(f"Db index: {db_index}")

                elif parsed_code == self.code_db_size:
                    db_size = _parse_length(f)
                    expire_size = _parse_length(f)
                    print(f"Database size: {db_size} keys, {expire_size} with expiry")
                    
                    for _ in range(db_size):
                        value_type = f.read(1)
                        
                        if value_type == b'\x00':
                            key_length = _parse_length(f)
                            key = f.read(key_length)
                            value_length = _parse_length(f)
                            value = f.read(value_length)
                            data[key] = value
                            print(f"Loaded key: {key} = {value}")
                        else:
                            print(f"Unsupported value type: {value_type.hex()}")
                            raise NotImplementedError(f"Value type {value_type.hex()} not supported")

                parsed_code = f.read(1)

        return data


def _parse_length(f) -> int:
    encoded_size_header = f.read(1)[0]
    first_two_bits = encoded_size_header >> 6

    if first_two_bits == 0b00:
        print("The length is the remaining 6 bits of the byte.")
        return encoded_size_header & 0b00111111

    elif first_two_bits == 0b01:
        print("The length is the next 14 bits, big endian")
        next_byte = f.read(1)[0]
        return ((encoded_size_header & 0b00111111) << 8) | next_byte

    elif first_two_bits == 0b10:
        print("Ignore the remaining 6 bits of first byte, length is the next 4 bytes, in big-endian")
        return int.from_bytes(f.read(4), "big")

    elif first_two_bits == 0b11:
        print("The remaining 6 bits specify a type of string encoding.")
        encoding_type = encoded_size_header & 0b00111111
        if encoding_type == 0:
            return 1
        elif encoding_type == 1:
            return 2
        elif encoding_type == 2:
            return 4
        else:
            raise ValueError(f"Unrecognized type: {first_two_bits}/{encoding_type}")
    else:
        raise ValueError(f"Unrecognized header {first_two_bits}")
