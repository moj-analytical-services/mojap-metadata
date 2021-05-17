# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## v1.2.0 - 2021-05-17

### Changed

- Extended pyarrow dependency to v4
- Metadata now allows for `bool` as an alias for `bool_` and `list` as an alias for `list_`. Works with all Converters.

### Added

- Metadata methods that can alter columns property in Metadata class. These are `column_names`, `update_column` and `remove_column` - see README for more details.
- Metadata property that forces the location of partitions `force_partition_order` - see README for more details.
- `ArrowConverter` now has a `generate_to_meta` method.

## v1.1.1 - 2021-03-16

### Changed
- Allow dependency for pyarrow v2 and v3

## v1.1.0 - 2021-03-11

### Added
- `$schema` is now set to a url when reading/writing from a dict.
- Updated schema where there was a typo from `struct_type`. (schema url updated to this release).
- method to Metadata object that sets type_category for any column where it is not set.

