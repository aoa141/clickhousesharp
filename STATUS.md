  ClickHouseSharp Function Implementation Status

  Implemented Functions (126 unique + 19 aliases)
  ┌─────────────────┬─────────────┬──────────────────┬──────────┐
  │    Category     │ Implemented │ ClickHouse Total │ Coverage │
  ├─────────────────┼─────────────┼──────────────────┼──────────┤
  │ Math            │ 24          │ 37               │ ~65%     │
  ├─────────────────┼─────────────┼──────────────────┼──────────┤
  │ String          │ 22          │ 86+              │ ~26%     │
  ├─────────────────┼─────────────┼──────────────────┼──────────┤
  │ Date/Time       │ 20          │ 100+             │ ~20%     │
  ├─────────────────┼─────────────┼──────────────────┼──────────┤
  │ Array           │ 16          │ 80+              │ ~20%     │
  ├─────────────────┼─────────────┼──────────────────┼──────────┤
  │ Aggregate       │ 16          │ 197+             │ ~8%      │
  ├─────────────────┼─────────────┼──────────────────┼──────────┤
  │ Type Conversion │ 11          │ 30+              │ ~37%     │
  ├─────────────────┼─────────────┼──────────────────┼──────────┤
  │ Conditional     │ 6           │ 10+              │ ~60%     │
  ├─────────────────┼─────────────┼──────────────────┼──────────┤
  │ Tuple           │ 2           │ 10+              │ ~20%     │
  ├─────────────────┼─────────────┼──────────────────┼──────────┤
  │ Map             │ 3           │ 15+              │ ~20%     │
  ├─────────────────┼─────────────┼──────────────────┼──────────┤
  │ Misc            │ 6           │ 50+              │ ~12%     │
  └─────────────────┴─────────────┴──────────────────┴──────────┘
  ---
  Not Implemented by Category

  Math (13 missing):
  - cbrt, degrees, radians, hypot, factorial, gcd, lcm
  - sinh, cosh, tanh, asinh, acosh, atanh
  - erf, erfc, lgamma, tgamma, widthBucket
  - exp2, exp10, log1p, intExp2, intExp10

  String (64+ missing):
  - Encoding: base64Encode/Decode, base32Encode/Decode, base58Encode/Decode
  - UTF-8: lengthUTF8, lowerUTF8, upperUTF8, reverseUTF8, substringUTF8
  - Padding: leftPad, rightPad, leftPadUTF8, rightPadUTF8
  - Similarity: jaroSimilarity, jaroWinklerSimilarity, editDistance, damerauLevenshteinDistance
  - CRC/Hash: CRC32, CRC64
  - HTML/XML: decodeHTMLComponent, encodeXMLComponent, decodeXMLComponent
  - ascii, space, initcap, soundex, firstLine, basename
  - concatWithSeparator, substringIndex, regexpExtract
  - Many more...

  Date/Time (80+ missing):
  - Add/Subtract: addDays, addMonths, addYears, subtractDays, etc. (have dateAdd/dateSub but not individual functions)
  - Change: changeYear, changeMonth, changeDay, changeHour, etc.
  - Formatting: formatDateTimeInJodaSyntax, monthName, dateName
  - Advanced: dateTrunc, age, serverTimezone
  - makeDate, makeDateTime, makeDateTime64
  - now64, nowInBlock
  - YYYYMMDDToDate, YYYYMMDDhhmmssToDateTime
  - Julian day functions, timezone functions

  Array (64+ missing):
  - Higher-order: arrayMap, arrayFilter, arrayAll, arrayExists, arrayFirst, arrayLast
  - Aggregation: arrayAvg, arrayMin, arrayMax, arraySum, arrayCount, arrayProduct
  - Transform: arrayFlatten, arrayCompact, arrayFill, arrayFold, arrayReduce
  - Set ops: arrayIntersect, arrayExcept, arrayUnion
  - Math: arrayCumSum, arrayDifference, arrayDotProduct
  - arrayEnumerate, arrayEnumerateUniq, arrayJoin
  - arrayRotateLeft, arrayRotateRight, arrayShiftLeft, arrayShiftRight
  - arrayRandomSample, arrayPartialSort, arrayShuffle

  Aggregate (181+ missing):
  - Statistical: stddevPop, stddevSamp, varPop, varSamp, covarPop, covarSamp, corr
  - Quantiles: quantile, quantileExact, median, quantileTiming, quantileDeterministic
  - Histogram: histogram, sparkBar
  - Top-K: topK, topKWeighted
  - Approximate: uniqCombined, uniqHLL12, uniqTheta
  - Group: groupArrayInsertAt, groupArrayMovingAvg, groupArraySample, groupBitAnd, groupBitOr, groupBitXor
  - Window: exponentialMovingAverage, retention, sequenceMatch, sequenceCount
  - Other: anyHeavy, first_value, last_value, sumMap, minMap, maxMap, avgWeighted
  - -If variants: minIf, maxIf, etc.
  - -Array variants: sumArray, avgArray, etc.
  - -Merge/-State variants for combining aggregates

  ---
  Entirely Missing Categories
  ┌───────────────────────┬────────────────────────────────────────────────────────────────────────────────────────┐
  │       Category        │                                        Examples                                        │
  ├───────────────────────┼────────────────────────────────────────────────────────────────────────────────────────┤
  │ JSON                  │ JSONExtract, JSONExtractString, JSONExtractInt, JSONHas, JSONLength, simpleJSONExtract │
  ├───────────────────────┼────────────────────────────────────────────────────────────────────────────────────────┤
  │ URL                   │ domain, topLevelDomain, path, queryString, extractURLParameter, cutURLParameter        │
  ├───────────────────────┼────────────────────────────────────────────────────────────────────────────────────────┤
  │ IP Address            │ IPv4NumToString, IPv4StringToNum, IPv6NumToString, toIPv4, isIPv4, IPv4CIDRToRange     │
  ├───────────────────────┼────────────────────────────────────────────────────────────────────────────────────────┤
  │ UUID                  │ UUIDStringToNum, UUIDNumToString, serverUUID                                           │
  ├───────────────────────┼────────────────────────────────────────────────────────────────────────────────────────┤
  │ Hash                  │ MD5, SHA1, SHA256, sipHash64, cityHash64, xxHash32, murmurHash2_64                     │
  ├───────────────────────┼────────────────────────────────────────────────────────────────────────────────────────┤
  │ Geo                   │ greatCircleDistance, geoDistance, pointInPolygon, geohashEncode, geohashDecode         │
  ├───────────────────────┼────────────────────────────────────────────────────────────────────────────────────────┤
  │ Encoding              │ hex, unhex, bin, unbin, char, ord                                                      │
  ├───────────────────────┼────────────────────────────────────────────────────────────────────────────────────────┤
  │ Bit                   │ bitAnd, bitOr, bitXor, bitNot, bitShiftLeft, bitShiftRight, bitCount, bitTest          │
  ├───────────────────────┼────────────────────────────────────────────────────────────────────────────────────────┤
  │ Regular Expression    │ match, extract, extractAll, replaceRegexpOne, replaceRegexpAll, regexpQuoteMeta        │
  ├───────────────────────┼────────────────────────────────────────────────────────────────────────────────────────┤
  │ NULL handling         │ isNull, isNotNull, isZeroOrNull, isNaN, isFinite, isInfinite (some in conditional)     │
  ├───────────────────────┼────────────────────────────────────────────────────────────────────────────────────────┤
  │ External Dictionaries │ dictGet, dictGetOrDefault, dictHas, dictGetHierarchy                                   │
  ├───────────────────────┼────────────────────────────────────────────────────────────────────────────────────────┤
  │ Machine Learning      │ stochasticLinearRegression, stochasticLogisticRegression                               │
  ├───────────────────────┼────────────────────────────────────────────────────────────────────────────────────────┤
  │ Introspection         │ currentDatabase, currentUser, hostName, getMacro, getSetting                           │
  └───────────────────────┴────────────────────────────────────────────────────────────────────────────────────────┘
  ---
  Summary

  - Implemented: ~126 unique functions (145 with aliases)
  - ClickHouse Total: ~800+ functions
  - Overall Coverage: ~15-20%

  The core SQL functionality (basic math, strings, dates, aggregates, window functions) is well-covered for typical analytical queries. Missing are specialized functions for JSON parsing, geo
  operations, hashing, URL parsing, IP handling, and advanced statistical aggregates.