import csv

from pyspark_dnstapir.hll import HLLType, decode_hll
from pyspark_dnstapir.hll.decoder import _read_full_registers, _read_sparse_registers


def test_spec_examples():
    B = bytes.fromhex("016344B4C0")
    registers = _read_sparse_registers(B, log2_nb_registers=11, register_width=6)
    expected_registers = bytearray(1 << 11)
    expected_registers[11] = 6
    expected_registers[1099] = 19
    assert registers == bytes(expected_registers)

    B = bytes.fromhex("004430")
    registers = _read_full_registers(B, log2_nb_registers=2, register_width=5)
    assert registers == bytes([0, 1, 2, 3])


def test_decode_empty():
    decoded = decode_hll(bytes.fromhex("116A7F"))
    assert decoded.schema_version == 1
    assert decoded.type == HLLType.EMPTY
    assert decoded.registers is None
    assert decoded.explicit_values is None


def test_decode_sparse_and_full():
    with open("test-data/hll/hll_test_sparse_full.csv", newline="") as csvfile:
        reader = csv.reader(csvfile)
        next(reader)  # skip header
        for row in reader:
            serialized_hex, log2_nb_registers, register_width, sparse_enabled, is_sparse, registers_hex = row
            serialized = bytes.fromhex(serialized_hex)
            registers = bytes.fromhex(registers_hex)
            log2_nb_registers = int(log2_nb_registers)
            register_width = int(register_width)
            sparse_enabled = sparse_enabled == "true"
            is_sparse = is_sparse == "true"
            # is_sparse and sparse_enabled are not necessarily the same because SPARSE may be upgraded to FULL

            decoded = decode_hll(serialized)
            assert decoded.schema_version == 1
            assert decoded.type == HLLType.SPARSE if is_sparse else HLLType.FULL
            assert decoded.explicit_values is None
            assert decoded.log2_nb_registers == log2_nb_registers
            assert decoded.register_width == register_width
            assert decoded.sparse_enabled == sparse_enabled
            assert decoded.registers == registers


def test_decode_explicit():
    with open("test-data/hll/hll_test_explicit.csv", newline="") as csvfile:
        reader = csv.reader(csvfile)
        next(reader)  # skip header
        for row in reader:
            serialized_hex, explicit_values = row
            serialized = bytes.fromhex(serialized_hex)
            explicit_values = set(int(x) for x in explicit_values.split("|"))

            decoded = decode_hll(serialized)
            assert decoded.schema_version == 1
            assert set(decoded.explicit_values) == explicit_values
