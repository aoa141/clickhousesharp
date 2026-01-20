using ClickHouseSharp.Parsing.Ast;

namespace ClickHouseSharp.Types;

public static class TypeParser
{
    public static ClickHouseType Parse(string typeName)
    {
        var lexer = new Parsing.Lexer(typeName);
        var parser = new Parsing.Parser(lexer.Tokenize());
        // Create a minimal parser just for data types
        return ParseFromString(typeName.Trim());
    }

    public static ClickHouseType Parse(DataTypeNode node) => ParseNode(node);

    private static ClickHouseType ParseFromString(string typeName)
    {
        // Handle parameterized types
        var parenIndex = typeName.IndexOf('(');
        if (parenIndex > 0)
        {
            var baseName = typeName[..parenIndex].Trim();
            var paramsStr = typeName[(parenIndex + 1)..^1].Trim();

            return baseName.ToUpperInvariant() switch
            {
                "NULLABLE" => new NullableType(ParseFromString(paramsStr)),
                "ARRAY" => new ArrayType(ParseFromString(paramsStr)),
                "LOWCARDINALITY" => new LowCardinalityType(ParseFromString(paramsStr)),
                "FIXEDSTRING" => new FixedStringType(int.Parse(paramsStr)),
                "DECIMAL" or "DECIMAL32" or "DECIMAL64" or "DECIMAL128" or "DECIMAL256" => ParseDecimal(baseName, paramsStr),
                "DATETIME" when paramsStr.StartsWith("'") => new DateTimeType(paramsStr.Trim('\'')),
                "DATETIME64" => ParseDateTime64(paramsStr),
                "MAP" => ParseMap(paramsStr),
                "TUPLE" => ParseTuple(paramsStr),
                "ENUM8" or "ENUM16" => StringType.Instance, // Treat enums as strings for simplicity
                _ => throw new ArgumentException($"Unknown parameterized type: {baseName}")
            };
        }

        return typeName.ToUpperInvariant() switch
        {
            "INT8" or "TINYINT" => Int8Type.Instance,
            "INT16" or "SMALLINT" => Int16Type.Instance,
            "INT32" or "INT" or "INTEGER" => Int32Type.Instance,
            "INT64" or "BIGINT" => Int64Type.Instance,
            "UINT8" => UInt8Type.Instance,
            "UINT16" => UInt16Type.Instance,
            "UINT32" => UInt32Type.Instance,
            "UINT64" => UInt64Type.Instance,
            "FLOAT32" or "FLOAT" => Float32Type.Instance,
            "FLOAT64" or "DOUBLE" => Float64Type.Instance,
            "STRING" or "TEXT" or "VARCHAR" => StringType.Instance,
            "BOOL" or "BOOLEAN" => BoolType.Instance,
            "DATE" => DateType.Instance,
            "DATE32" => DateType.Instance,
            "DATETIME" => DateTimeType.Instance,
            "UUID" => UuidType.Instance,
            _ => throw new ArgumentException($"Unknown type: {typeName}")
        };
    }

    private static ClickHouseType ParseNode(DataTypeNode node)
    {
        var baseName = node.TypeName.ToUpperInvariant();

        // Handle types with type parameters
        if (node.TypeParameters is { Count: > 0 })
        {
            return baseName switch
            {
                "NULLABLE" => new NullableType(ParseNode(node.TypeParameters[0])),
                "ARRAY" => new ArrayType(ParseNode(node.TypeParameters[0])),
                "LOWCARDINALITY" => new LowCardinalityType(ParseNode(node.TypeParameters[0])),
                "MAP" when node.TypeParameters.Count >= 2 => new MapType(
                    ParseNode(node.TypeParameters[0]),
                    ParseNode(node.TypeParameters[1])),
                "TUPLE" => new TupleType(node.TypeParameters.Select(ParseNode).ToList()),
                _ => throw new ArgumentException($"Unknown parameterized type: {baseName}")
            };
        }

        // Handle types with numeric parameters
        if (node.NumericParameters is { Count: > 0 })
        {
            return baseName switch
            {
                "FIXEDSTRING" => new FixedStringType(node.NumericParameters[0]),
                "DECIMAL" => new DecimalType(
                    node.NumericParameters[0],
                    node.NumericParameters.Count > 1 ? node.NumericParameters[1] : 0),
                "DECIMAL32" => new DecimalType(node.NumericParameters[0], node.NumericParameters.Count > 1 ? node.NumericParameters[1] : 0),
                "DECIMAL64" => new DecimalType(node.NumericParameters[0], node.NumericParameters.Count > 1 ? node.NumericParameters[1] : 0),
                "DECIMAL128" => new DecimalType(node.NumericParameters[0], node.NumericParameters.Count > 1 ? node.NumericParameters[1] : 0),
                "DECIMAL256" => new DecimalType(node.NumericParameters[0], node.NumericParameters.Count > 1 ? node.NumericParameters[1] : 0),
                "DATETIME64" => new DateTime64Type(node.NumericParameters[0]),
                _ => throw new ArgumentException($"Unknown type with numeric parameters: {baseName}")
            };
        }

        // Simple types
        return baseName switch
        {
            "INT8" or "TINYINT" => Int8Type.Instance,
            "INT16" or "SMALLINT" => Int16Type.Instance,
            "INT32" or "INT" or "INTEGER" => Int32Type.Instance,
            "INT64" or "BIGINT" => Int64Type.Instance,
            "UINT8" => UInt8Type.Instance,
            "UINT16" => UInt16Type.Instance,
            "UINT32" => UInt32Type.Instance,
            "UINT64" => UInt64Type.Instance,
            "FLOAT32" or "FLOAT" => Float32Type.Instance,
            "FLOAT64" or "DOUBLE" => Float64Type.Instance,
            "STRING" or "TEXT" or "VARCHAR" => StringType.Instance,
            "BOOL" or "BOOLEAN" => BoolType.Instance,
            "DATE" => DateType.Instance,
            "DATE32" => DateType.Instance,
            "DATETIME" => DateTimeType.Instance,
            "UUID" => UuidType.Instance,
            _ => throw new ArgumentException($"Unknown type: {node.TypeName}")
        };
    }

    private static DecimalType ParseDecimal(string baseName, string paramsStr)
    {
        var parts = paramsStr.Split(',').Select(s => int.Parse(s.Trim())).ToArray();
        return new DecimalType(parts[0], parts.Length > 1 ? parts[1] : 0);
    }

    private static DateTime64Type ParseDateTime64(string paramsStr)
    {
        var parts = paramsStr.Split(',').Select(s => s.Trim()).ToArray();
        var precision = int.Parse(parts[0]);
        var timezone = parts.Length > 1 ? parts[1].Trim('\'') : null;
        return new DateTime64Type(precision, timezone);
    }

    private static MapType ParseMap(string paramsStr)
    {
        var depth = 0;
        var commaIndex = -1;
        for (int i = 0; i < paramsStr.Length; i++)
        {
            var c = paramsStr[i];
            if (c == '(') depth++;
            else if (c == ')') depth--;
            else if (c == ',' && depth == 0)
            {
                commaIndex = i;
                break;
            }
        }

        if (commaIndex < 0)
            throw new ArgumentException($"Invalid Map type parameters: {paramsStr}");

        var keyType = ParseFromString(paramsStr[..commaIndex].Trim());
        var valueType = ParseFromString(paramsStr[(commaIndex + 1)..].Trim());
        return new MapType(keyType, valueType);
    }

    private static TupleType ParseTuple(string paramsStr)
    {
        var types = new List<ClickHouseType>();
        var names = new List<string>();
        var depth = 0;
        var start = 0;

        for (int i = 0; i <= paramsStr.Length; i++)
        {
            if (i == paramsStr.Length || (paramsStr[i] == ',' && depth == 0))
            {
                var part = paramsStr[start..i].Trim();

                // Check for named element: "name Type"
                var spaceIndex = part.IndexOf(' ');
                if (spaceIndex > 0 && !part[..spaceIndex].Contains('('))
                {
                    names.Add(part[..spaceIndex]);
                    types.Add(ParseFromString(part[(spaceIndex + 1)..].Trim()));
                }
                else
                {
                    names.Add($"_{types.Count + 1}");
                    types.Add(ParseFromString(part));
                }

                start = i + 1;
            }
            else if (paramsStr[i] == '(')
            {
                depth++;
            }
            else if (paramsStr[i] == ')')
            {
                depth--;
            }
        }

        return new TupleType(types, names);
    }
}
