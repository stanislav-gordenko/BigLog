using System.Diagnostics.CodeAnalysis;

namespace BigLog.Utilities
{
    public static class DictionaryHelper
    {
        /// <summary>
        /// Dictionary to array conversion
        /// </summary>
        /// <param name="dict"></param>
        /// <returns></returns>
        [return: NotNullIfNotNull(nameof(dict))]
        public static T?[]? ToArray<T>(this Dictionary<string, T?>? dict)
        {
            if (dict == null) return null;
            T?[] arr = new T?[dict.Count];
            for (int i = 0; i < dict.Count; i++)
            {
                arr[i] = dict.ElementAt(i).Value;
            }
            return arr;
        }
        /// <summary>
        /// Selection of some values ​​from a dictionary, such as keys
        /// </summary>
        /// <param name="Row"></param>
        /// <param name="fields"></param>
        /// <returns></returns>
        public static T?[] SelectValues<T>(this Dictionary<string, T?> Row, string[]? fields)
        {
            if (Row == null || fields == null) return [];
            T?[] values = new T?[fields.Length];
            for (int i = 0; i < fields.Length; i++)
            {
                if (!Row.TryGetValue(fields[i], out T? value)) values[i] = default;
                else values[i] = value;
            }
            return values;
        }
        /// <summary>
        /// Check if any element of a dictionary is null
        /// </summary>
        /// <param name="dict"></param>
        /// <param name="keys"></param>
        /// <returns></returns>
        public static bool DictionaryIsAnyNull<T>([NotNullWhen(false)] this Dictionary<string, T?>? dict, string[] keys)
        {
            if (dict == null) return true;
            if (keys == null) return false;
            foreach (string key in keys)
            {
                if (dict[key] == null) return true;
            }
            return false;
        }
        /// <summary>
        /// Checking if all the dictionary values ​​listed are the same
        /// </summary>
        /// <param name="dict"></param>
        /// <param name="keys"></param>
        /// <returns></returns>
        public static bool DictionaryAllEqual<T>([NotNullWhen(false)] this Dictionary<string, T?>? dict, string[] keys)
        {
            if (dict == null) return true;
            if (keys == null) return true;
            for (int i = 0; i < keys.Length; i++)
            {
                for (int j = i + 1; j < keys.Length; j++)
                {
                    if (dict[keys[i]]?.ToString() != dict[keys[j]]?.ToString()) return false;
                }
            }
            return true;
        }
        /// <summary>
        /// Append a static element to each dictionary in a list
        /// </summary>
        /// <param name="list"></param>
        /// <param name="Key"></param>
        /// <param name="value"></param>
        public static void Add<T>(this List<Dictionary<string, T>> list, string key, T value)
        {
            if (list == null) return;
            foreach (Dictionary<string, T> Row in list)
            {
                Row.Add(key, value);
            }
        }
        /// <summary>
        /// Removing pairs with unnecessary keys from the dictionary. The keys may not even be in the list.
        /// </summary>
        /// <param name="Row"></param>
        /// <param name="ExcludeKeys"></param>
        /// <returns></returns>
        public static Dictionary<string, T?> ExcludeKeys<T>(this Dictionary<string, T?> Row, string[] ExcludeKeys)
        {
            if (ExcludeKeys == null) return Row;
            Dictionary<string, T?> ReducedRow = [];
            foreach (KeyValuePair<string, T?> elem in Row)
            {
                for (int j = 0; j < ExcludeKeys.Length; j++)
                {
                    if (elem.Key == ExcludeKeys[j]) goto nextField;
                }
                ReducedRow.Add(elem.Key, elem.Value);
            nextField:;
            }
            return ReducedRow;
        }
        /// <summary>
        /// Filling the first dictionary with the values ​​of the second dictionary if they are empty in the first, except for the specified fields
        /// </summary>
        /// <param name="Row"></param>
        /// <param name="Row1"></param>
        /// <param name="ExcludeFields"></param>
        /// <returns></returns>
        public static Dictionary<string, T?> FillFrom<T>(this Dictionary<string, T?> Row,
            Dictionary<string, T?> Row1, string[] ExcludeFields)
        {
            foreach (KeyValuePair<string, T?> elem in Row)
            {
                if (!ExcludeFields.Contains(elem.Key))
                {
                    if (elem.Value == null)
                        Row[elem.Key] = Row1[elem.Key];
                    else if (string.IsNullOrWhiteSpace(elem.Value.ToString()))
                        Row[elem.Key] = Row1[elem.Key];
                    else if (elem.Value.GetType() == typeof(int) && Row1[elem.Key] != null)
                    {
                        _ = int.TryParse(elem.Value.ToString(), out int val);
                        _ = int.TryParse(Row1[elem.Key]!.ToString(), out int val1);
                        if (val == 0) Row[elem.Key] = (T)Convert.ChangeType(val1, typeof(T));
                    }
                }
            }
            return Row;
        }
        /// <summary>
        /// Comparison of two lists
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="Entity1"></param>
        /// <param name="Entity2"></param>
        /// <returns></returns>
        public static bool DictionaryEquals<T>(this Dictionary<string, T>? dict1, Dictionary<string, T>? dict2, Func<T, T, bool> CompareMethod, bool IsOrderSufficient = false)
        {
            if (dict1 == null && dict2 == null) return true;
            if (dict1 == null || dict2 == null || dict1.Count != dict2.Count) return false;
            Dictionary<string, T>? dict3 = null;
            if (!IsOrderSufficient)
                dict3 = dict2.Keys.ToDictionary(r => r, r => dict2[r]);
            for (int i = 0; i < dict1.Count; i++)
            {
                if (IsOrderSufficient)
                {
                    if (dict1.ElementAt(i).Key != dict2.ElementAt(i).Key ||
                        !CompareMethod(dict1.ElementAt(i).Value, dict2.ElementAt(i).Value))
                        return false;
                }
                else if (dict3 != null)
                {
                    for (int j = dict3.Count - 1; j >= 0; j--)
                    {
                        if (dict1.ElementAt(i).Key != dict3.ElementAt(j).Key &&
                            CompareMethod(dict1.ElementAt(i).Value, dict3.ElementAt(j).Value))
                        {
                            dict3.Remove(dict3.ElementAt(j).Key);
                            goto NextI;
                        }
                    }
                    return false;
                }
            NextI:;
            }
            return true;
        }
        /// <summary>
        /// Setting a value to a dictionary, adding the key if it doesn't exist
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="dictionary"></param>
        /// <param name="Key"></param>
        /// <param name="Value"></param>
        public static void SetValue<T>(this Dictionary<string, T?> dictionary, string Key, T? Value)
        {
            if (dictionary == null) return;
            if (!dictionary.TryAdd(Key, Value))
                dictionary[Key] = Value;
        }
        /// <summary>
        /// Setting a value to a dictionary by adding a key and calculating the value with a function only if the condition is met
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="dictionary"></param>
        /// <param name="Key"></param>
        /// <param name="Value"></param>
        public static void SetValue<T>(this Dictionary<string, T?> dictionary, string Key, Func<T?> GetValue)
        {
            if (dictionary == null) return;
            T? Value = GetValue();
            if (!dictionary.TryAdd(Key, Value))
                dictionary[Key] = Value;
        }
        /// <summary>
        /// Setting a value to a dictionary by adding a key and calculating the value with a function only if the condition is met
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="dictionary"></param>
        /// <param name="Key"></param>
        /// <param name="Value"></param>
        public static async Task SetValueAsync<T>(this Dictionary<string, T?> dictionary, string Key, Func<Task<T?>> GetValueAsync)
        {
            if (dictionary == null) return;
            T? Value = await GetValueAsync();
            if (!dictionary.TryAdd(Key, Value))
                dictionary[Key] = Value;
        }
        /// <summary>
        /// Select a value from a dictionary, returns the default value if the key not found
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="dictionary"></param>
        /// <param name="Key"></param>
        /// <param name="Value"></param>
        public static T GetValue<T>(this Dictionary<string, T> dictionary, string Key, T DefaultValue)
        {
            if (dictionary == null || !dictionary.TryGetValue(Key, out T? value)) return DefaultValue;
            return value;
        }
        /// <summary>
        /// Copying properties from dictionary to dictionary
        /// </summary>
        /// <param name="source"></param>
        /// <param name="prefix">Selecting the keys with a prefix only</param>
        /// <returns></returns>
        public static Dictionary<string, string?> DictionaryToDictionary(this IDictionary<string, object?> source, string prefix)
        {
            Dictionary<string, string?> toDictionary = [];
            foreach (KeyValuePair<string, object?> pair in source)
            {
                if (pair.Key.StartsWith(prefix, StringComparison.InvariantCultureIgnoreCase))
                {
                    string? value = pair.Value?.ToString();
                    toDictionary.Add(pair.Key, value);
                }
            }
            return toDictionary;
        }
        /// <summary>
        /// Copying a dictionary to a new dictionary
        /// </summary>
        /// <param name="source"></param>
        /// <returns></returns>
        public static IDictionary<string, object?> CopyDictionary(this IDictionary<string, object?> source)
        {
            Dictionary<string, object?> dict = [];
            foreach (KeyValuePair<string, object?> pair in source)
            {
                dict.Add(pair.Key, pair.Value);
            }
            return dict;
        }
        /// <summary>
        /// Merging two dictionaries, the values ​​of the second one take precedence
        /// </summary>
        /// <param name="source"></param>
        /// <returns></returns>
        public static IDictionary<string, T> AddRange<T>(this IDictionary<string, T> target, IDictionary<string, T> source)
        {
            if (source == null) return target;
            if (target == null) return source;
            foreach (KeyValuePair<string, T> pair in source)
            {
                if (target.ContainsKey(pair.Key)) target[pair.Key] = pair.Value;
                else target.Add(pair.Key, pair.Value);
            }
            return target;
        }
        /// <summary>
        /// Compare two lists and return equal values ​​and two remainders of unequal values
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="list1"></param>
        /// <param name="list2"></param>
        /// <param name="rest1"></param>
        /// <param name="rest2"></param>
        /// <returns></returns>
        public static Dictionary<string, T> SelectEqualWithRests<T>(this Dictionary<string, T>? dict1, Dictionary<string, T>? dict2,
            Func<T, T, bool> CompareMethod, out Dictionary<string, T> rest1, out Dictionary<string, T> rest2)
        {
            Dictionary<string, T> equal = [];
            rest1 = [];
            rest2 = [];
            if (dict1 == null || dict2 == null) return equal;
            foreach (KeyValuePair<string, T> pair in dict1)
            {
                if (dict2.TryGetValue(pair.Key, out T? value) && CompareMethod(value, pair.Value))
                    equal.Add(pair.Key, pair.Value);
                else rest1.Add(pair.Key, pair.Value);
            }
            foreach (KeyValuePair<string, T> pair in dict2)
            {
                if (!equal.TryGetValue(pair.Key, out T? value) || !CompareMethod(value, pair.Value))
                    rest2.Add(pair.Key, pair.Value);
            }
            return equal;
        }
        /// <summary>
        /// Checking if any element of a dictionary is null
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="dict"></param>
        /// <returns></returns>
        public static bool DictionaryIsAnyNull<T>([NotNullWhen(false)] this Dictionary<string, T>? dict)
        {
            if (dict == null) return true;
            foreach (KeyValuePair<string, T> elem in dict)
            {
                if (string.IsNullOrWhiteSpace(elem.Value?.ToString())) return true;
            }
            return false;
        }
        /// <summary>
        /// Check if any element of a dictionary is null
        /// </summary>
        /// <param name="dict"></param>
        /// <param name="keys"></param>
        /// <returns></returns>
        public static bool DictionaryIsAnyNull([NotNullWhen(false)] this Dictionary<string, object?>? dict, string[] keys)
        {
            if (dict == null) return true;
            if (keys == null) return false;
            foreach (string key in keys)
            {
                if (dict[key] == null) return true;
            }
            return false;
        }
        /// <summary>
        /// Removing all keys from the dictionary except the necessary ones
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="Row"></param>
        /// <param name="KeepKeys"></param>
        /// <returns></returns>
        public static Dictionary<string, T> KeepOnlyKeys<T>(this Dictionary<string, T> Row, string[] KeepKeys)
        {
            Dictionary<string, T> ReducedRow = [];
            if (KeepKeys == null) return ReducedRow;
            foreach (KeyValuePair<string, T> elem in Row)
            {
                for (int j = 0; j < KeepKeys.Length; j++)
                {
                    if (elem.Key == KeepKeys[j]) { ReducedRow.Add(elem.Key, elem.Value); break; }
                }
            }
            return ReducedRow;
        }
        /// <summary>
        /// Rotate a dictionary of lists 90 degrees and turn it into a list of dictionaries
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="source"></param>
        /// <returns></returns>
        public static List<Dictionary<string, T>> ToListOfDictionary<T>(this Dictionary<string, List<T>> source)
        {
            List<Dictionary<string, T>> result = [];
            for (int i = 0; i < source.Count; i++) // ключи
            {
                for (int j = 0; j < source.ElementAt(i).Value.Count; j++) // списки
                {
                    if (i == 0)
                        result.Add([]);
                    result[j].Add(source.ElementAt(i).Key, source.ElementAt(i).Value[j]);
                }
            }
            return result;
        }
    }
}
