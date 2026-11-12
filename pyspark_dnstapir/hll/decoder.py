from collections.abc import Iterator
from dataclasses import dataclass
from enum import IntEnum


class HLLType(IntEnum):
    UNDEFINED = 0
    EMPTY = 1
    EXPLICIT = 2
    SPARSE = 3
    FULL = 4


@dataclass
class DecodedHLL:
    """
    Represents a decoded HyperLogLog structure and its metadata.

    If the type is SPARSE or FULL, the `registers` field contains the decoded register values (one byte per register),
    and `explicit_values` is None.

    If the type is EXPLICIT, the `explicit_values` field contains the decoded explicit values,
    and `registers` is None.

    If the type is EMPTY or UNDEFINED, both `registers` and `explicit_values` are None.
    """

    schema_version: int
    type: HLLType
    register_width: int
    log2_nb_registers: int
    explicit_cutoff: int
    sparse_enabled: bool
    registers: bytes | None
    explicit_values: tuple[int, ...] | None

    @property
    def nb_registers(self) -> int:
        return 1 << self.log2_nb_registers


def decode_hll(data: bytes) -> DecodedHLL:
    """
    Decode a serialized HyperLogLog structure in AGKN (Aggregate Knowledge) format.
    """
    if len(data) < 3:
        raise ValueError("HLL data must contain at least three bytes")
    V = data[0]
    version = V >> 4  # "V is the 'version' byte, indicating the schema version in the top nibble"
    if version != 1:
        raise ValueError(f"Unsupported HLL schema version: {version}")
    try:
        hll_type = HLLType(V & 15)  # "The bottom nibble of the 'version' byte V indicates the type"
    except ValueError as e:
        raise ValueError(f"Unsupported HLL type: {V & 15}") from e

    # "The schema-specific bytes S* have the following structure: PCB*"
    P = data[1]  # "P is the required 'parameter' byte."
    C = data[2]  # "C is the required 'cutoff' byte.""
    B = data[3:]  # "B is a 'data' byte. The 'data' bytes layout is specified per type."

    register_width = (P >> 5) + 1  # "the highest 3 bits are used to encode the integer value registerWidth - 1"
    log2_nb_registers = P & 31  # "the remaining 5 bits encode the integer value log2(numberOfRegisters)"
    if log2_nb_registers < 4:
        raise ValueError("Invalid log2(numberOfRegisters) value: must be in the range 4 to 31")

    # "The 'cutoff' byte C encodes parameters defining the EXPLICIT to SPARSE, and SPARSE to FULL promotions."
    sparse_enabled = (C & 0b01000000) != 0  # "1 bit (second highest bit) indicating the boolean value sparseEnabled"

    explicit_cutoff = C & 63
    # "6 bits (lowest six bits) as an unsigned big-endian integer explicitCutoff that can take on the
    # values 0, 63, or 1 to 31 inclusive."
    if explicit_cutoff != 0 and explicit_cutoff != 63 and not (1 <= explicit_cutoff <= 31):
        raise ValueError(f"Invalid explicit cutoff value: {explicit_cutoff}")

    if hll_type in (HLLType.UNDEFINED, HLLType.EMPTY):
        if len(B) != 0:
            raise ValueError("UNDEFINED or EMPTY HLL type should not use data bytes.")
        registers = None
        explicit_values = None
    elif hll_type == HLLType.EXPLICIT:
        explicit_values = _read_explicit_values(B)
        registers = None
    elif hll_type == HLLType.SPARSE:
        if not sparse_enabled:
            raise ValueError("SPARSE HLL type requires sparseEnabled to be set")
        registers = _read_sparse_registers(B, log2_nb_registers, register_width)
        explicit_values = None
    elif hll_type == HLLType.FULL:
        registers = _read_full_registers(B, log2_nb_registers, register_width)
        explicit_values = None
    else:
        raise ValueError(f"Unsupported HLL type: {hll_type}")

    return DecodedHLL(
        schema_version=version,
        type=hll_type,
        log2_nb_registers=log2_nb_registers,
        register_width=register_width,
        sparse_enabled=sparse_enabled,
        explicit_cutoff=explicit_cutoff,
        registers=registers,
        explicit_values=explicit_values,
    )


def _read_explicit_values(B: bytes) -> tuple[int, ...]:
    # "The layout of the 'data' bytes is: (B{8}){0,256}, that is 0-256 blocks of 8 bytes."
    # While these blocks are interpreted as signed 64-bit integers according to spec,
    # the Go implementation accepts them as unsigned in AddRaw, so we also decode them as unsigned here.
    if len(B) % 8 != 0:
        raise ValueError("Explicit data length must be a multiple of 8 bytes")
    contents = tuple(int.from_bytes(B[i : i + 8], byteorder="big", signed=False) for i in range(0, len(B), 8))
    return contents


def _words(B: bytes, width: int) -> Iterator[int]:
    if width <= 0:
        raise ValueError("width must be greater than zero")
    accumulator = 0
    available = 0
    mask = (1 << width) - 1

    for byte in B:
        accumulator = (accumulator << 8) | byte
        available += 8

        while available >= width:
            available -= width
            yield (accumulator >> available) & mask

        accumulator &= (1 << available) - 1

    if accumulator != 0:
        raise ValueError("Nonzero padding bits in HLL data")


def _read_sparse_registers(B: bytes, log2_nb_registers: int, register_width: int) -> bytes:
    registers = bytearray(1 << log2_nb_registers)
    width = log2_nb_registers + register_width
    prev_index = -1
    for word in _words(B, width):
        index = (
            word >> register_width
        )  # "The index is encoded in the highest log2(numberOfRegisters) bits of the short-word."
        value = word & (
            (1 << register_width) - 1
        )  # "The value is encoded in the lowest registerWidth bits of the short-word."

        if index <= prev_index:
            # "The short-words are stored in ascending index order."
            raise ValueError("Sparse HLL indices must be strictly increasing")
        prev_index = index

        registers[index] = value

    return bytes(registers)


def _read_full_registers(B: bytes, log2_nb_registers: int, register_width: int) -> bytes:
    registers = bytes(_words(B, register_width))
    nb_registers = 1 << log2_nb_registers
    if len(registers) != nb_registers:
        raise ValueError(f"Expected {nb_registers} full registers, got {len(registers)}")
    return registers
