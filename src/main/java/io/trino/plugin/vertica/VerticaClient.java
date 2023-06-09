/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.vertica;

import com.google.common.math.LongMath;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.ObjectWriteFunction;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.plugin.jdbc.mapping.IdentifierMapping;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.trino.plugin.jdbc.DecimalConfig.DecimalMapping.ALLOW_OVERFLOW;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.getDecimalDefaultScale;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.getDecimalRounding;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.getDecimalRoundingMode;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.PredicatePushdownController.DISABLE_PUSHDOWN;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.charReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.charWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.dateColumnMappingUsingSqlDate;
import static io.trino.plugin.jdbc.StandardColumnMappings.dateWriteFunctionUsingLocalDate;
import static io.trino.plugin.jdbc.StandardColumnMappings.decimalColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.fromTrinoTimestamp;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.longDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.realWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.shortDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.timeWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.timestampColumnMappingUsingSqlTimestampWithRounding;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.Timestamps.round;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.math.RoundingMode.UNNECESSARY;

public class VerticaClient
        extends BaseJdbcClient
{
    private static final Logger log = Logger.get(VerticaClient.class);
    private static final int ARRAY_RESULT_SET_VALUE_COLUMN = 2;
    private static final String DUPLICATE_TABLE_SQLSTATE = "42P07";
    private static final int POSTGRESQL_MAX_SUPPORTED_TIMESTAMP_PRECISION = 6;
    private static final int PRECISION_OF_UNSPECIFIED_DECIMAL = 0;

    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS");

    @Inject
    public VerticaClient(
            BaseJdbcConfig config,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier remoteQueryModifier)
    {
        super("\"", connectionFactory, queryBuilder, config.getJdbcTypesMappedToVarchar(), identifierMapping, remoteQueryModifier, true);
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        String jdbcTypeName = typeHandle.getJdbcTypeName()
                .orElseThrow(() -> new TrinoException(JDBC_ERROR, "Type name is missing: " + typeHandle));

        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }

        switch (typeHandle.getJdbcType()) {
            case Types.BIT:
            case Types.BOOLEAN:
                return Optional.of(booleanColumnMapping());

            case Types.SMALLINT:
                return Optional.of(smallintColumnMapping());

            case Types.INTEGER:
                return Optional.of(integerColumnMapping());

            case Types.BIGINT:
                return Optional.of(bigintColumnMapping());

            case Types.DOUBLE:
                return Optional.of(doubleColumnMapping());

            case Types.NUMERIC: {
                int columnSize = typeHandle.getRequiredColumnSize();
                int precision;
                int decimalDigits = typeHandle.getDecimalDigits().orElse(0);
                if (getDecimalRounding(session) == ALLOW_OVERFLOW) {
                    if (columnSize == PRECISION_OF_UNSPECIFIED_DECIMAL) {
                        // decimal type with unspecified scale - up to 131072 digits before the decimal point; up to 16383 digits after the decimal point)
                        return Optional.of(decimalColumnMapping(createDecimalType(Decimals.MAX_PRECISION, getDecimalDefaultScale(session)), getDecimalRoundingMode(session)));
                    }
                    precision = columnSize;
                    if (precision > Decimals.MAX_PRECISION) {
                        int scale = min(decimalDigits, getDecimalDefaultScale(session));
                        return Optional.of(decimalColumnMapping(createDecimalType(Decimals.MAX_PRECISION, scale), getDecimalRoundingMode(session)));
                    }
                }
                precision = columnSize + max(-decimalDigits, 0); // Map decimal(p, -s) (negative scale) to decimal(p+s, 0).
                if (columnSize == PRECISION_OF_UNSPECIFIED_DECIMAL || precision > Decimals.MAX_PRECISION) {
                    break;
                }
                return Optional.of(decimalColumnMapping(createDecimalType(precision, max(decimalDigits, 0)), UNNECESSARY));
            }

            case Types.CHAR:
                return Optional.of(charColumnMapping(typeHandle.getRequiredColumnSize()));

            case Types.VARCHAR:
                return Optional.of(varcharColumnMapping(typeHandle.getRequiredColumnSize()));

            case Types.BINARY:
            case Types.VARBINARY:
                return Optional.of(varbinaryColumnMapping());

            case Types.DATE:
                return Optional.of(dateColumnMappingUsingSqlDate());

            case Types.TIME:
                return Optional.of(timeColumnMapping(typeHandle.getRequiredDecimalDigits()));

            case Types.TIMESTAMP:
                return Optional.of(timestampColumnMappingUsingSqlTimestampWithRounding(TIMESTAMP_MILLIS));

            /*case Types.ARRAY:
                Optional<ColumnMapping> columnMapping = arrayToTrinoType(session, connection, typeHandle);
                if (columnMapping.isPresent()) {
                    return columnMapping;
                }
                break;*/
        }

        if (getUnsupportedTypeHandling(session) == CONVERT_TO_VARCHAR) {
            return mapToUnboundedVarchar(typeHandle);
        }

        return Optional.empty();
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (type == BOOLEAN) {
            return WriteMapping.booleanMapping("boolean", booleanWriteFunction());
        }

        if (type == TINYINT) {
            // PostgreSQL has no type corresponding to tinyint
            return WriteMapping.longMapping("smallint", tinyintWriteFunction());
        }
        if (type == SMALLINT) {
            return WriteMapping.longMapping("smallint", smallintWriteFunction());
        }
        if (type == INTEGER) {
            return WriteMapping.longMapping("integer", integerWriteFunction());
        }
        if (type == BIGINT) {
            return WriteMapping.longMapping("bigint", bigintWriteFunction());
        }

        if (type == REAL) {
            return WriteMapping.longMapping("real", realWriteFunction());
        }
        if (type == DOUBLE) {
            return WriteMapping.doubleMapping("double precision", doubleWriteFunction());
        }

        if (type instanceof DecimalType decimalType) {
            String dataType = format("decimal(%s, %s)", decimalType.getPrecision(), decimalType.getScale());
            if (decimalType.isShort()) {
                return WriteMapping.longMapping(dataType, shortDecimalWriteFunction(decimalType));
            }
            return WriteMapping.objectMapping(dataType, longDecimalWriteFunction(decimalType));
        }

        if (type instanceof CharType) {
            return WriteMapping.sliceMapping("char(" + ((CharType) type).getLength() + ")", charWriteFunction());
        }

        if (type instanceof VarcharType varcharType) {
            String dataType;
            if (varcharType.isUnbounded()) {
                dataType = "varchar";
            }
            else {
                dataType = "varchar(" + varcharType.getBoundedLength() + ")";
            }
            return WriteMapping.sliceMapping(dataType, varcharWriteFunction());
        }
        if (VARBINARY.equals(type)) {
            return WriteMapping.sliceMapping("bytea", varbinaryWriteFunction());
        }

        if (type == DATE) {
            return WriteMapping.longMapping("date", dateWriteFunctionUsingLocalDate());
        }

        if (type instanceof TimeType timeType) {
            if (timeType.getPrecision() <= POSTGRESQL_MAX_SUPPORTED_TIMESTAMP_PRECISION) {
                return WriteMapping.longMapping(format("time(%s)", timeType.getPrecision()), timeWriteFunction(timeType.getPrecision()));
            }
            return WriteMapping.longMapping(format("time(%s)", POSTGRESQL_MAX_SUPPORTED_TIMESTAMP_PRECISION), timeWriteFunction(POSTGRESQL_MAX_SUPPORTED_TIMESTAMP_PRECISION));
        }

        if (type instanceof TimestampType timestampType) {
            if (timestampType.getPrecision() <= POSTGRESQL_MAX_SUPPORTED_TIMESTAMP_PRECISION) {
                verify(timestampType.getPrecision() <= TimestampType.MAX_SHORT_PRECISION);
                return WriteMapping.longMapping(format("timestamp(%s)", timestampType.getPrecision()), VerticaClient::shortTimestampWriteFunction);
            }
            verify(timestampType.getPrecision() > TimestampType.MAX_SHORT_PRECISION);
            return WriteMapping.objectMapping(format("timestamp(%s)", POSTGRESQL_MAX_SUPPORTED_TIMESTAMP_PRECISION), longTimestampWriteFunction());
        }
        if (type instanceof TimestampWithTimeZoneType timestampWithTimeZoneType) {
            if (timestampWithTimeZoneType.getPrecision() <= POSTGRESQL_MAX_SUPPORTED_TIMESTAMP_PRECISION) {
                String dataType = format("timestamptz(%d)", timestampWithTimeZoneType.getPrecision());
                if (timestampWithTimeZoneType.getPrecision() <= TimestampWithTimeZoneType.MAX_SHORT_PRECISION) {
                    return WriteMapping.longMapping(dataType, shortTimestampWithTimeZoneWriteFunction());
                }
                return WriteMapping.objectMapping(dataType, longTimestampWithTimeZoneWriteFunction());
            }
            return WriteMapping.objectMapping(format("timestamptz(%d)", POSTGRESQL_MAX_SUPPORTED_TIMESTAMP_PRECISION), longTimestampWithTimeZoneWriteFunction());
        }
        /*if (type.equals(uuidType)) {
            return WriteMapping.sliceMapping("uuid", uuidWriteFunction());
        }*/
        /*if (type instanceof ArrayType arrayType && getArrayMapping(session) == AS_ARRAY) {
            Type elementType = arrayType.getElementType();
            String elementDataType = toWriteMapping(session, elementType).getDataType();
            return WriteMapping.objectMapping(elementDataType + "[]", arrayWriteFunction(session, elementType, getArrayElementPgTypeName(session, this, elementType)));
        }*/

        throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
    }

    private static ColumnMapping charColumnMapping(int charLength)
    {
        if (charLength > CharType.MAX_LENGTH) {
            return varcharColumnMapping(charLength);
        }
        CharType charType = createCharType(charLength);
        return ColumnMapping.sliceMapping(
                charType,
                charReadFunction(charType),
                charWriteFunction(),
                DISABLE_PUSHDOWN);
    }

    private static ColumnMapping varcharColumnMapping(int varcharLength)
    {
        VarcharType varcharType = varcharLength <= VarcharType.MAX_LENGTH
                ? createVarcharType(varcharLength)
                : createUnboundedVarcharType();
        return ColumnMapping.sliceMapping(
                varcharType,
                varcharReadFunction(varcharType),
                varcharWriteFunction(),
                DISABLE_PUSHDOWN);
    }

    private static ColumnMapping timeColumnMapping(int precision)
    {
        verify(precision <= 6, "Unsupported precision: %s", precision); // PostgreSQL limit but also assumption within this method
        return ColumnMapping.longMapping(
                createTimeType(precision),
                (resultSet, columnIndex) -> {
                    LocalTime time = resultSet.getObject(columnIndex, LocalTime.class);
                    long nanosOfDay = time.toNanoOfDay();
                    if (nanosOfDay == NANOSECONDS_PER_DAY - 1) {
                        // PostgreSQL's 24:00:00 is returned as 23:59:59.999999999, regardless of column precision
                        nanosOfDay = NANOSECONDS_PER_DAY - LongMath.pow(10, 9 - precision);
                    }

                    long picosOfDay = nanosOfDay * PICOSECONDS_PER_NANOSECOND;
                    return round(picosOfDay, 12 - precision);
                },
                timeWriteFunction(precision),
                // Pushdown disabled because PostgreSQL distinguishes TIME '24:00:00' and TIME '00:00:00' whereas Trino does not.
                DISABLE_PUSHDOWN);
    }

    private static void shortTimestampWriteFunction(PreparedStatement statement, int index, long epochMicros)
            throws SQLException
    {
        LocalDateTime localDateTime = fromTrinoTimestamp(epochMicros);
        statement.setTimestamp(index, Timestamp.valueOf(localDateTime.toString()));
    }

    private static ObjectWriteFunction longTimestampWriteFunction()
    {
        return ObjectWriteFunction.of(LongTimestamp.class, ((statement, index, timestamp) -> {
            // PostgreSQL supports up to 6 digits of precision
            //noinspection ConstantConditions
            verify(POSTGRESQL_MAX_SUPPORTED_TIMESTAMP_PRECISION == 6);

            long epochMicros = timestamp.getEpochMicros();
            if (timestamp.getPicosOfMicro() >= PICOSECONDS_PER_MICROSECOND / 2) {
                epochMicros++;
            }
            shortTimestampWriteFunction(statement, index, epochMicros);
        }));
    }

    private static LongWriteFunction shortTimestampWithTimeZoneWriteFunction()
    {
        return (statement, index, value) -> {
            // PostgreSQL does not store zone information in "timestamp with time zone" data type
            long millisUtc = unpackMillisUtc(value);
            statement.setTimestamp(index, new Timestamp(millisUtc));
        };
    }

    private static ObjectWriteFunction longTimestampWithTimeZoneWriteFunction()
    {
        return ObjectWriteFunction.of(
                LongTimestampWithTimeZone.class,
                (statement, index, value) -> {
                    // PostgreSQL does not store zone information in "timestamp with time zone" data type
                    long epochSeconds = floorDiv(value.getEpochMillis(), MILLISECONDS_PER_SECOND);
                    long nanosOfSecond = floorMod(value.getEpochMillis(), MILLISECONDS_PER_SECOND) * NANOSECONDS_PER_MILLISECOND + value.getPicosOfMilli() / PICOSECONDS_PER_NANOSECOND;
                    statement.setObject(index, OffsetDateTime.ofInstant(Instant.ofEpochSecond(epochSeconds, nanosOfSecond), UTC_KEY.getZoneId()));
                });
    }
}
