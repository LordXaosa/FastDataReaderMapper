using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace FastDataReaderMapper
{
    public static class Mapper
    {
        public static List<T> Map<T>(this IDataReader reader) where T : new()
        {
            List<T> list = new List<T>();
            HashSet<string> tableFields = new HashSet<string>();
            for (int i = 0; i < reader.FieldCount; i++)
                tableFields.Add(reader.GetName(i).ToLower());

            PropertyInfo[] properties = typeof(T).GetProperties();
            Dictionary<PropertyInfo, string> nameDict = new Dictionary<PropertyInfo, string>();
            Dictionary<PropertyInfo, Type> propertyType = new Dictionary<PropertyInfo, Type>();
            Dictionary<PropertyInfo, Action<T, object>> setters = new Dictionary<PropertyInfo, Action<T, object>>();
            Dictionary<string, string> lowerCaseNames = new Dictionary<string, string>();
            foreach (PropertyInfo pi in properties)
            {
                string name = pi.GetCustomAttribute<ColumnAttribute>()?.Name ?? pi.Name;
                nameDict.Add(pi, name);
                lowerCaseNames.Add(name, name.ToLower());
                propertyType.Add(pi, Nullable.GetUnderlyingType(pi.PropertyType) ?? pi.PropertyType);
                setters.Add(pi, BuildUntypedSetter<T>(pi));
            }
            while (reader.Read())
            {
                T result = new T();
                foreach (PropertyInfo pi in properties)
                {
                    string name = nameDict[pi];

                    if (!string.IsNullOrEmpty(name) && tableFields.Contains(lowerCaseNames[name]))
                    {
                        object val = reader[name];
                        Type dataType = val.GetType();
                        if (pi.PropertyType.IsAssignableFrom(dataType))
                            setters[pi](result, val);
                        else if (dataType == typeof(DBNull))
                            setters[pi](result, null);
                        else
                        {
                            object safeValue = (val == null) ? null : Convert.ChangeType(val, propertyType[pi]);
                            setters[pi](result, safeValue);
                        }
                    }
                }
                list.Add(result);
            }
            return list;
        }

        private static Action<T, object> BuildUntypedSetter<T>(PropertyInfo propertyInfo)
        {
            var targetType = propertyInfo.DeclaringType;
            var methodInfo = propertyInfo.GetSetMethod();
            var exTarget = Expression.Parameter(targetType, "t");
            var exValue = Expression.Parameter(typeof(object), "p");
            var exBody = Expression.Call(exTarget, methodInfo,
               Expression.Convert(exValue, propertyInfo.PropertyType));
            var lambda = Expression.Lambda<Action<T, object>>(exBody, exTarget, exValue);
            var action = lambda.Compile();
            return action;
        }

        private static Func<T, object> BuildUntypedGetter<T>(PropertyInfo propertyInfo)
        {
            var targetType = propertyInfo.DeclaringType;
            var methodInfo = propertyInfo.GetGetMethod();
            var returnType = methodInfo.ReturnType;

            var exTarget = Expression.Parameter(targetType, "t");
            var exBody = Expression.Call(exTarget, methodInfo);
            var exBody2 = Expression.Convert(exBody, typeof(object));

            var lambda = Expression.Lambda<Func<T, object>>(exBody2, exTarget);

            var action = lambda.Compile();
            return action;
        }

        public static async IAsyncEnumerable<T> MapAsync<T>(this IDataReader reader, [EnumeratorCancellation]CancellationToken cancellationToken = default) where T : new()
        {
            Console.WriteLine("Data aquired to mapper. Processing...");
            HashSet<string> tableFields = new HashSet<string>();
            for (int i = 0; i < reader.FieldCount; i++)
                tableFields.Add(reader.GetName(i).ToLower());

            PropertyInfo[] properties = typeof(T).GetProperties();
            Dictionary<PropertyInfo, string> nameDict = new Dictionary<PropertyInfo, string>();
            Dictionary<PropertyInfo, Type> propertyType = new Dictionary<PropertyInfo, Type>();
            Dictionary<PropertyInfo, Action<T, object>> setters = new Dictionary<PropertyInfo, Action<T, object>>();
            Dictionary<string, string> lowerCaseNames = new Dictionary<string, string>();
            foreach (PropertyInfo pi in properties)
            {
                string name = pi.GetCustomAttribute<ColumnAttribute>()?.Name ?? pi.Name;
                nameDict.Add(pi, name);
                lowerCaseNames.Add(name, name.ToLower());
                propertyType.Add(pi, Nullable.GetUnderlyingType(pi.PropertyType) ?? pi.PropertyType);
                setters.Add(pi, BuildUntypedSetter<T>(pi));
            }
            while (reader.Read())
            {
                T result = new T();
                foreach (PropertyInfo pi in properties)
                {
                    string name = nameDict[pi];

                    if (!string.IsNullOrEmpty(name) && tableFields.Contains(lowerCaseNames[name]))
                    {
                        object val = reader[name];
                        Type dataType = val.GetType();
                        if (pi.PropertyType.IsAssignableFrom(dataType))
                            setters[pi](result, val);
                        else if (dataType == typeof(DBNull))
                            setters[pi](result, null);
                        else
                        {
                            object safeValue = (val == null) ? null : Convert.ChangeType(val, propertyType[pi]);
                            setters[pi](result, safeValue);
                        }
                    }
                }
                yield return result;
            }
        }
    }
}
