Vendored from the upstream Zstandard project:
https://github.com/facebook/zstd

Source snapshot:
- package version previously used by parquet-swift: 1.5.6

Why this is vendored:
- Xcode's strict module verification rejects the upstream SwiftPM module map
  because it declares `config_macros`.
- parquet-swift uses a local `module.modulemap` that exports only the public
  zstd headers without those configuration macros.

Update approach:
1. Replace the files in `common/`, `compress/`, `decompress/`, `dictBuilder/`,
   plus `zstd.h`, `zdict.h`, and `zstd_errors.h` from the upstream release.
2. Keep the local `module.modulemap`.
3. Re-run `swift test` and `xcodebuild -scheme parquet-swift -destination generic/platform=macOS build`.
