// RLEEncoder - Hybrid RLE/Bit-Packing encoder for Parquet
//
// Licensed under the Apache License, Version 2.0

import Foundation

/// RLE/Bit-Packing Hybrid encoder for Parquet dictionary indices
///
/// This encoder implements the hybrid RLE/bit-packing encoding used for:
/// - Dictionary indices in dictionary-encoded columns
/// - Definition and repetition levels (future use)
///
/// # Output Format (Data Page)
///
/// ```
/// <bit-width: 1 byte>
/// <runs... (variable length)>
/// ```
///
/// # Run Encoding
///
/// Each run is either:
/// - **RLE run**: Header `(count << 1)`, followed by single value (bit-width bits)
/// - **Bit-packed run**: Header `(count << 1) | 1`, followed by bit-packed values
///
/// Bit-packed runs contain groups of 8 values packed together.
///
/// # Usage
///
/// ```swift
/// var encoder = RLEEncoder(bitWidth: 4)  // 4 bits per index (max 16 unique values)
/// encoder.encode([0, 0, 0, 1, 2, 3, 4, 5])
/// let data = encoder.flush()
/// ```
public final class RLEEncoder {
    private let bitWidth: Int
    private var buffer: Data
    private var currentRun: Run?

    // Configuration
    private let minRepeatCount = 8  // Minimum repetitions to trigger RLE
    private let maxBitPackedRunSize = 64  // Max values in a bit-packed run (must be multiple of 8)

    /// Initialize RLE encoder
    /// - Parameter bitWidth: Bit width for values (0-32)
    public init(bitWidth: Int) {
        self.bitWidth = bitWidth
        self.buffer = Data()
        self.buffer.append(UInt8(bitWidth))  // Write bit-width header
    }

    /// Encode a sequence of values
    /// - Parameter values: Array of UInt32 indices to encode
    public func encode(_ values: [UInt32]) {
        for value in values {
            encodeOne(value)
        }
    }

    /// Encode a single value
    /// - Parameter value: Value to encode
    public func encodeOne(_ value: UInt32) {
        guard let run = currentRun else {
            // Start new run
            currentRun = Run(value: value, count: 1, consecutiveCount: 1)
            return
        }

        if run.isRLE {
            // Currently in RLE run
            if value == run.value {
                // Continue RLE run
                currentRun!.count += 1
            } else {
                // End RLE run, start new one
                flushCurrentRun()
                currentRun = Run(value: value, count: 1, consecutiveCount: 1)
            }
        } else {
            // Currently in bit-packed run
            let newConsecutiveCount = (value == run.value) ? run.consecutiveCount + 1 : 1

            if value == run.value && newConsecutiveCount >= minRepeatCount {
                // Found enough repetitions, convert to RLE
                // Write out bit-packed values before the repeat
                let bitPackedValues = run.bitPackedValues[..<(run.bitPackedValues.count - (minRepeatCount - 1))]
                if !bitPackedValues.isEmpty {
                    writeBitPackedRun(Array(bitPackedValues))
                }

                // Start RLE run with the repeating value
                currentRun = Run(value: value, count: minRepeatCount, consecutiveCount: minRepeatCount, isRLE: true)
            } else if run.bitPackedValues.count >= maxBitPackedRunSize {
                // Bit-packed run is full, flush it
                flushCurrentRun()
                currentRun = Run(value: value, count: 1, consecutiveCount: 1)
            } else {
                // Continue bit-packed run
                currentRun!.bitPackedValues.append(value)
                currentRun!.value = value
                currentRun!.count += 1
                currentRun!.consecutiveCount = newConsecutiveCount
            }
        }
    }

    /// Flush any remaining data and return encoded bytes
    /// - Returns: Complete RLE-encoded data (including bit-width header)
    public func flush() -> Data {
        flushCurrentRun()
        return buffer
    }

    // MARK: - Private Methods

    private func flushCurrentRun() {
        guard let run = currentRun else { return }

        if run.isRLE {
            writeRLERun(value: run.value, count: run.count)
        } else {
            writeBitPackedRun(run.bitPackedValues)
        }

        currentRun = nil
    }

    private func writeRLERun(value: UInt32, count: Int) {
        // RLE run header: (count << 1)
        let header = count << 1
        writeVarint(UInt32(header))

        // Write the repeated value (bit-width bits)
        writeValue(value)
    }

    private func writeBitPackedRun(_ values: [UInt32]) {
        guard !values.isEmpty else { return }

        // Pad to multiple of 8
        var paddedValues = values
        while paddedValues.count % 8 != 0 {
            paddedValues.append(0)
        }

        let count = paddedValues.count / 8  // Number of groups of 8

        // Bit-packed run header: (count << 1) | 1
        let header = (count << 1) | 1
        writeVarint(UInt32(header))

        // Write bit-packed values
        writeBitPackedValues(paddedValues)
    }

    private func writeBitPackedValues(_ values: [UInt32]) {
        guard bitWidth > 0 else { return }

        var bitBuffer: UInt64 = 0
        var bitsInBuffer = 0

        for value in values {
            // Add value to bit buffer
            bitBuffer |= UInt64(value) << bitsInBuffer
            bitsInBuffer += bitWidth

            // Write out complete bytes
            while bitsInBuffer >= 8 {
                buffer.append(UInt8(bitBuffer & 0xFF))
                bitBuffer >>= 8
                bitsInBuffer -= 8
            }
        }

        // Write remaining bits
        if bitsInBuffer > 0 {
            buffer.append(UInt8(bitBuffer & 0xFF))
        }
    }

    private func writeValue(_ value: UInt32) {
        // Write value using bit-width bits
        if bitWidth == 0 { return }

        let bytesNeeded = (bitWidth + 7) / 8
        for i in 0..<bytesNeeded {
            buffer.append(UInt8((value >> (i * 8)) & 0xFF))
        }
    }

    private func writeVarint(_ value: UInt32) {
        var val = value
        while val >= 0x80 {
            buffer.append(UInt8(val & 0x7F | 0x80))
            val >>= 7
        }
        buffer.append(UInt8(val))
    }

    // MARK: - Run Tracking

    private struct Run {
        var value: UInt32
        var count: Int
        var isRLE: Bool
        var bitPackedValues: [UInt32]
        var consecutiveCount: Int  // Track consecutive identical values for RLE detection

        init(value: UInt32, count: Int, consecutiveCount: Int = 1, isRLE: Bool = false) {
            self.value = value
            self.count = count
            self.consecutiveCount = consecutiveCount
            self.isRLE = isRLE
            self.bitPackedValues = isRLE ? [] : [value]
        }
    }
}
