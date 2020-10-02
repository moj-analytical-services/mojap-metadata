# mojap-metadata

Draft project on creating our own metadata package

This package currently has two submodules.

## Metadata 

Look it is one word! This module defines the generic metadata schemas that everything else is based off of. We use this generalised metadata to define our data in a centralised standard. This is basically the exact same as etl_manager.meta (but with the glue database creation and DDL creation taken out of it that bit is now in the converters section)

## Converters

The idea of this submodule is to add more and more converters. Converters have two base functions (see the `mojap_metadata.converters.base_converter`). One takes a thing and converts outputs the Metadata object with definitions based on said "thing". The other takes the a Metadata object and produces a "thing". I don't know if we want to be specific by the word thing as atm it is quite broad in the converters I've created as examples (worth noting how each converter is a Child of the base_converter that does nothing):

- **pandas_converter:** Either infers what the metadata should be from a given dataframe or casts the columns in a dataframe to match the specified metadata.
- **glue_converter:**: This is the other part of the etl_manager TableMeta class that I have gutted out of Metadata class here. It either takes the Glue DDL and converts it to our metadata or creates the glue DDL from our metadata. If we were to do this I'd imagine internals of etl_manager would just use the converters from here and the metadata class to then actually push the table DDLs to glue (as that _might_ and/or _should_ be out of scope for the converters???)
- 

## Package Management

Converters are seperated out so that you can only install what you need. Let's say we had an Oracle converter that we integrated to this package that required the oracle_cx package. You can imagine that this might start to get unweildy the more converters we add in for postgres, mysql, pandas, etc. However, all we would do in this case is the following:

1. Make sure that additional packages are only referenced in the converter that uses them. Note in that Pandas is only imported in the `pandas_converter` and boto3 (other was stuff) is only used in the `glue_converter` module. When you install this package it won't come with pandas or boto3. It will only come with the bare packages needed to for the metadata submodule. If you want to install the extras then you specify it by using the `extras` option in your package install...

2. If you look at the `pyproject.toml` you'll see there is the option to install additional packages for your converter needs. If you needed the pandas_converter functionality you can then `pip install mojap-metadata[pandas]`. For glue `pip install mojap-metadata[glue]` or both pandas and glue `pip install mojap-metadata[glue,pandas]`.

>FYI not stating we should use poetry - I just know how to create extras defintions in there.
