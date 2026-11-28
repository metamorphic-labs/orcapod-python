# Errors

Exceptions and warnings defined in `orcapod.errors`.

## Input, schema, and value validation

These exceptions and warnings report invalid inputs, schemas, or values.

::: orcapod.errors.InputValidationError

::: orcapod.errors.DuplicateTagError

::: orcapod.errors.SchemaInconsistencyError

::: orcapod.errors.TypeIdentityError

::: orcapod.errors.RecursiveTypeError

::: orcapod.errors.CanonicalOrderError
::: orcapod.errors.LogicalSetCanonicalOrderError

::: orcapod.errors.UnsupportedValueError

::: orcapod.errors.ValueConversionWarning

::: orcapod.errors.UnstorableDatetimeError

::: orcapod.errors.NaiveDatetimeError

::: orcapod.errors.SubMicrosecondDatetimeError

::: orcapod.errors.InstantOutOfRangeError

::: orcapod.errors.NotADatetimeError

::: orcapod.errors.NullInNonNullableColumnError

## Sources

These exceptions and warnings report problems identifying, binding, or reading sources.

::: orcapod.errors.RowIdentifierError

::: orcapod.errors.SourceTooLargeError

::: orcapod.errors.UnboundSourceError

::: orcapod.errors.SourceSpecMismatchError

::: orcapod.errors.InconsistentSourceError

::: orcapod.errors.CursorInvalidatedError

::: orcapod.errors.SourceCheckFailedWarning

::: orcapod.errors.SourceNameWarning

::: orcapod.errors.SourceVersionWarning

::: orcapod.errors.CachedSourceIdMismatchError

::: orcapod.errors.SourceSlotNameCollisionError

## Pipeline execution

These exceptions and warnings report problems running pipelines, processing rows, or tracing results.

::: orcapod.errors.PipelineJobRequiredError

::: orcapod.errors.DataFunctionUnavailableError

::: orcapod.errors.OutputTypeError

::: orcapod.errors.OutputContainerTypeError

::: orcapod.errors.NoOutputsDeclaredError

::: orcapod.errors.RowFailuresWarning

::: orcapod.errors.RestrictedAggregationWarning

::: orcapod.errors.SemiJoinIgnoredColumnWarning

::: orcapod.errors.RejoinDifferingColumnWarning

::: orcapod.errors.PolarsFilterWarning

::: orcapod.errors.UpstreamFailedError

::: orcapod.errors.EmptyDataAccessError

::: orcapod.errors.EmptyDataHashMissingError

::: orcapod.errors.EphemeralResultMissingError

::: orcapod.errors.ProvenanceInconsistencyError

## Databases and connectors

These exceptions and warnings report storage, schema, and connector problems.

::: orcapod.errors.OrcapodDatabaseError

::: orcapod.errors.OrcapodDatabaseReadError

::: orcapod.errors.OrcapodDatabaseWriteError

::: orcapod.errors.StoredTableSchemaError

::: orcapod.errors.SideEffectLogDataContextError

::: orcapod.errors.OrcapodDatabaseContentWarning

::: orcapod.errors.UnstorableColumnError

::: orcapod.errors.UnstorableTableNameError

::: orcapod.errors.TableSchemaError

::: orcapod.errors.TableListingNotSupportedError

::: orcapod.errors.UnrecordedTableSchemaWarning

::: orcapod.errors.NullabilityRecordMismatchError

::: orcapod.errors.SpiralDBWriteNotSupportedError

::: orcapod.errors.SpiralDBObserverNotSupportedError

::: orcapod.errors.SpiralDBSignInError

## Feature availability and stability

These exceptions and warnings report optional dependencies and features still in development.

::: orcapod.errors.MissingOptionalDependencyError

::: orcapod.errors.ProvisionalHashWarning

::: orcapod.errors.InDevelopmentWarning
