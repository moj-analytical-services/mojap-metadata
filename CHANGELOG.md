# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## v1.15.2 - 2024-08-02

- add option to convert columns from camel/pascal case to snake case

## v1.15.1 - 2024-06-18

- updated README.md formatting in GlueConverter

## v1.15.0 - 2024-06-18

- updated metadata schema
- added functionality to write and pull table properties in Glue Catalog

## v1.14.3 - 2023-11-21

- update pypi action
- more dependencies for security

## v1.14.2 - 2023-11-14

- update dependencies for security

## v1.14.1 - 2023-09-13

- Remove spaces in decimal types in GlueConverter

## v1.14.0 - 2023-09-04

- Updated metadata schema to v1.4.0

## v1.13.1 - 2023-07-27

### Changed

- Updated dependencies to allow for python 3.11

## v1.13.0 - 2023-07-06

### Changed

- Added new AWS iceberg converter along with tests

## v1.2.2 - 2021-09-22

### Changed

- Updated project dependencies to align with other packages we heavily use

## v1.2.1 - 2021-06-07

### Changed

- Fixed bug where unpacking complex types (aka types with `<>`) did not correctly return contents of brackets.
- Added parameter `field_sep` to `converters._flatten_and_convert_complex_data_type` as Glue schemas fail if spaces are in complex data type definitions. Glue schemas now have no spaces before or after `,` when creating complex data types.

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

