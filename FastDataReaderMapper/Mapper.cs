using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Linq.Expressions;
using System.Reflection;
using System.Reflection.Emit;
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
            PrepareType(reader, out PropertyInfo[] properties, out HashSet<string> tableFields,
                out Dictionary<PropertyInfo, (string Name, Type PropertyType, Action<T, object> Setter, string LowerCasename, int? Order)> header);
            while (reader.Read())
            {
                list.Add(MapRow(reader, properties, tableFields, header));
            }
            return list;
        }
        public static async IAsyncEnumerable<T> MapAsync<T>(this IDataReader reader, [EnumeratorCancellation]CancellationToken cancellationToken = default) where T : new()
        {
            PrepareType(reader, out PropertyInfo[] properties, out HashSet<string> tableFields,
                out Dictionary<PropertyInfo, (string Name, Type PropertyType, Action<T, object> Setter, string LowerCasename, int? Order)> header);
            while (reader.Read())
            {
                if (cancellationToken.IsCancellationRequested)
                    throw new OperationCanceledException(cancellationToken);
                yield return MapRow(reader, properties, tableFields, header);
            }
        }

        private static void PrepareType<T>(IDataReader reader, out PropertyInfo[] properties, out HashSet<string> tableFields,
            out Dictionary<PropertyInfo, (string Name, Type PropertyType, Action<T, object> Setter, string LowerCasename, int? Order)> header) where T : new()
        {
            tableFields = new HashSet<string>();
            try
            {
                for (int i = 0; i < reader.FieldCount; i++)
                    tableFields.Add(reader.GetName(i).ToLower());
            }
            catch (NotSupportedException) { }
            catch (NotImplementedException) { }

            properties = typeof(T).GetProperties();
            header = new Dictionary<PropertyInfo, (string Name, Type PropertyType, Action<T, object> Setter, string LowerCasename, int? Order)>();
            foreach (PropertyInfo pi in properties)
            {
                ColumnAttribute col = pi.GetCustomAttribute<ColumnAttribute>();
                int? order = col?.Order==-1?null:col?.Order;
                string name = col?.Name ?? pi.Name;
                header.Add(pi, (name, Nullable.GetUnderlyingType(pi.PropertyType) ?? pi.PropertyType, BuildUntypedSetter<T>(pi), name.ToLower(), order));
            }
        }

        private static T MapRow<T>(IDataRecord row, PropertyInfo[] properties, HashSet<string> tableFields,
            Dictionary<PropertyInfo, (string Name, Type PropertyType, Action<T, object> Setter, string LowerCasename, int? Order)> header) where T : new()
        {
            T result = new T();
            foreach (PropertyInfo pi in properties)
            {
                (string Name, Type PropertyType, Action<T, object> Setter, string LowerCasename, int? Order) tuple = header[pi];
                string name = tuple.Name;

                if (!string.IsNullOrEmpty(name) && ((tuple.Order.HasValue && tuple.Order < row.FieldCount) || tableFields.Contains(tuple.LowerCasename)))
                {
                    object val = tuple.Order != null ? row[tuple.Order.Value] : row[name];
                    if (val == null)
                        continue;
                    Type dataType = val.GetType();
                    if (pi.PropertyType.IsAssignableFrom(dataType))
                        tuple.Setter(result, val);
                    else if (dataType == typeof(DBNull))
                        tuple.Setter(result, null);
                    else
                    {
                        object safeValue = (val == null) ? null : Convert.ChangeType(val, tuple.PropertyType);
                        tuple.Setter(result, safeValue);
                    }
                }
            }
            return result;
        }
        private static Action<T, object> BuildUntypedSetter<T>(PropertyInfo pi) where T : new()
        {
            /*var targetType = pi.DeclaringType;
            var methodInfo = pi.GetSetMethod();
            var exTarget = Expression.Parameter(targetType, "t");
            var exValue = Expression.Parameter(typeof(object), "p");
            var exBody = Expression.Call(exTarget, methodInfo,
               Expression.Convert(exValue, pi.PropertyType));
            var lambda = Expression.Lambda<Action<T, object>>(exBody, exTarget, exValue);
            var action = lambda.Compile();
            return action;*/
            DynamicMethod method = new DynamicMethod(
                "PropertySetter",
                typeof(void),
                new[] { typeof(T), typeof(object) },
                Assembly.GetExecutingAssembly().ManifestModule);
            ILGenerator il = method.GetILGenerator(100);
            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Ldarg_1);
            //if (pi.PropertyType.IsValueType)
            if (pi.PropertyType == typeof(DateTime))
                il.Emit(OpCodes.Unbox_Any, pi.PropertyType);
            il.EmitCall(OpCodes.Callvirt, pi.GetSetMethod(), null);
            il.Emit(OpCodes.Ret);
            return (Action<T, object>)method.CreateDelegate(typeof(Action<T, object>));
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
    }
}
