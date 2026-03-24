// swift-tools-version: 5.9
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "parquet-swift",
    platforms: [
        .macOS(.v13),      // macOS Ventura - async/await, modern APIs
        .iOS(.v16),        // iOS 16 - full modern Swift support
        .watchOS(.v9),     // watchOS 9
        .tvOS(.v16)        // tvOS 16
    ],
    products: [
        // The main Parquet library
        .library(
            name: "Parquet",
            targets: ["Parquet"]
        ),
    ],
    dependencies: [
        // Pure Swift Snappy implementation
        .package(url: "https://github.com/codelynx/snappy-swift.git", from: "1.0.1"),
    ],
    targets: [
        // Vendored Zstandard C library. We keep the module name as `libzstd`
        // so the existing Swift codec and tests do not need API changes.
        .target(
            name: "libzstd",
            path: "Sources/libzstd",
            exclude: ["LICENSE.zstd"],
            sources: ["common", "compress", "decompress", "dictBuilder"],
            publicHeadersPath: ".",
            cSettings: [
                .headerSearchPath("."),
                // Pre-define visibility macros so the #ifndef guards in zstd.h
                // don't trigger Xcode's "configuration macro" validation error.
                .define("ZSTDLIB_VISIBLE", to: ""),
                .define("ZSTDLIB_HIDDEN", to: ""),
                .define("ZSTD_CLEVEL_DEFAULT", to: "3"),
                .define("ZDICTLIB_VISIBLE", to: ""),
                .define("ZDICTLIB_HIDDEN", to: ""),
                .define("ZSTDERRORLIB_VISIBLE", to: ""),
                .define("ZSTDERRORLIB_HIDDEN", to: ""),
            ]
        ),

        // Main Parquet implementation
        .target(
            name: "Parquet",
            dependencies: [
                .product(name: "SnappySwift", package: "snappy-swift"),
                "libzstd",
            ],
            path: "Sources/Parquet",
            exclude: ["Reader/StructSemantics.md"]
        ),

        // Test suite
        .testTarget(
            name: "ParquetTests",
            dependencies: ["Parquet"],
            path: "Tests/ParquetTests",
            resources: [
                .copy("Fixtures")
            ]
        ),
        .testTarget(
            name: "ParquetPublicAPITests",
            dependencies: ["Parquet"],
            path: "Tests/ParquetPublicAPITests"
        ),
    ]
)
