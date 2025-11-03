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
        // Main Parquet implementation
        .target(
            name: "Parquet",
            dependencies: [
                .product(name: "SnappySwift", package: "snappy-swift")
            ],
            path: "Sources/Parquet"
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
    ]
)
