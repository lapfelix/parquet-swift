Vendored from the upstream Zstandard project:
https://github.com/facebook/zstd

Source snapshot:
- package version previously used by parquet-swift: 1.5.6

Why this is vendored:
- parquet-swift avoids the upstream package graph and ships a local
  `module.modulemap` for Xcode workspace compatibility.
- The local module map exports only the public zstd headers and explicitly
  declares the public configuration macros used by those headers.

Update approach:
1. Replace the files in `common/`, `compress/`, `decompress/`, `dictBuilder/`,
   plus `zstd.h`, `zdict.h`, and `zstd_errors.h` from the upstream release.
2. Keep the local `module.modulemap`.
3. Re-run `swift test` and `xcodebuild -scheme parquet-swift -destination generic/platform=macOS build`.
