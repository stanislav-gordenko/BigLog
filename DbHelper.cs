using BigLog.Data;
using BigLog.Data.Access;
using BigLog.Data.Models;
using BigLog.Localizers;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;
using Syncfusion.Blazor.Data;
using System.Collections;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Nodes;
using Wangkanai.Extensions;
using static BigLog.Utilities.Enums;

namespace BigLog.Utilities
{
    public static class DbHelper
    {
        public const string KeyAlreadyExistsToInsert = "The key already exists and could not be inserted";
        /// <summary>
        /// Adding a record with confirmation or rollback.
        /// For tables with a single numeric key, the key can be specified separately; otherwise, it should be embedded along with other properties.
        /// </summary>
        /// <typeparam name="TEntity"></typeparam>
        /// <typeparam name="TKey"></typeparam>
        /// <param name="dbSet"></param>
        /// <param name="db"></param>
        /// <param name="Entity"></param>
        /// <param name="NewKey">For integer keys, newKey does not need to be set; it is only used if Entity.Key = 0</param>
        /// <returns></returns>
        public static async Task<TEntity> InsertWithCommit<TEntity, TKey>(this DbSet<TEntity> dbSet,
            ApplicationDbContext db, TEntity Entity, TKey NewKey, CancellationToken token = default) where TEntity : class, new()
        {
            IReadOnlyKey? PrimaryKey = db.PrimaryKey(typeof(TEntity)); //r.GetPropertyValue(PrimaryKey.Properties[0].Name).ToString() == KeyValue
            Entity.SetEmptyStringsToNulls(true); // Spaces are considered empty, we do this in the existing object
            if (PrimaryKey == null)
                throw new Exception("Primary key could not be null");
            db.DetachAllEntries(typeof(TEntity), EntityState.Added);
            TEntity dbEntity = new();
            dbEntity.UpdateDbPropertiesFrom(Entity, db); // Remove reference properties to prevent adding records by reference. Do this in a new object to return an object with references.
            object?[] KeyValues = db.PrimaryValues(Entity);
            bool IsIdentityInsert1 = false;
            if (KeyValues.Length == 1 && typeof(TKey) == typeof(int)) // if the key is digital and consists of only one field, we can specify it separately
            {
                PropertyInfo? KeyProperty = string.IsNullOrEmpty(PrimaryKey.Properties?[0]?.Name) ? null : typeof(TEntity).GetProperty(PrimaryKey.Properties[0].Name!);
                TKey? KeyValue = (TKey?)KeyProperty?.GetValue(Entity);
                if (KeyValue != null && KeyProperty != null)
                {
                    if (typeof(TKey) == typeof(int))
                    {
                        KeyValue = Convert.ToInt32(NewKey) == 0 && Convert.ToInt32(KeyValue) != 0 ? KeyValue : NewKey;
                        // For tables with an auto-generated key, 0 is a temporary value, and if there are no other values ​​in the table, adding a row with 0 will give an error
                        if (Convert.ToInt32(KeyValue) == 0 && !await dbSet.AnyAsync(token))
                        {
                            KeyProperty.SetValue(Entity, 1); // there is no other way to assign KeyValue = 1
                            KeyValue = (TKey?)KeyProperty.GetValue(Entity);
                            IsIdentityInsert1 = true;
                        }
                    }
                    else if (NewKey != null) // string if the key value is not specified, it means it is already written in the Entity
                        KeyValue = NewKey;
                    KeyValues[0] = KeyValue!;
                    if (await dbSet.FindAsync(KeyValues, token) != null)
                        return await Task.FromException<TEntity>(new Exception(KeyAlreadyExistsToInsert));
                    KeyProperty.SetValue(Entity, KeyValue);
                    KeyProperty.SetValue(dbEntity, KeyValue);
                }
            }
            IExecutionStrategy strategy = db.Database.CreateExecutionStrategy();
            try
            {
                dbSet.Add(dbEntity);
            }
            catch (Exception ex)
            {
                return await Task.FromException<TEntity>(new Exception(ex.InnerException == null ? ex.Message : ex.InnerException.Message));
            }
            if (IsIdentityInsert1)
                await db.SaveChangesAsyncIdentityInsertOn(db.TableOfDbSet<TEntity>());
            else
            {
                try
                { // attempting to execute transaction elements asynchronously yields the error there are pending elements...
                    // var a = db.ChangeTracker.Entries().ToList(); // для проверки неотключенных сущностей
                    strategy.ExecuteInTransaction((db, dbSet),
                        operation: context =>
                        {
                            context.db.SaveChanges(acceptAllChangesOnSuccess: false);
                        },
                        verifySucceeded: context => context.dbSet.Find(KeyValues) != null);
                }
                catch (RetryLimitExceededException ex) //context => !context.Constants.Any(r => r.Id == Id)
                {
                    dbSet.Remove(dbEntity);
                    return await Task.FromException<TEntity>(new Exception(ex.InnerException == null ? ex.Message : ex.InnerException.Message));
                }
                catch (Exception ex)
                {
                    dbSet.Remove(dbEntity);
                    return await Task.FromException<TEntity>(new Exception(ex.InnerException == null ? ex.Message : ex.InnerException.Message));
                }
            }
            if (typeof(TKey) == typeof(int) && Convert.ToInt32(NewKey) == 0 && PrimaryKey.Properties?.Count == 1 && IsGeneratedKey<TEntity>(db))
            {
                int? KeyValue = (int?)dbSet.Max(PrimaryKey.Properties[0].Name);
                if (KeyValue != null) Entity.SetPropertyValue(PrimaryKey.Properties[0].Name, KeyValue);
            }
            db.ChangeTracker.AcceptAllChanges();
            return Entity;
        }
        /// <summary>
        /// Saving data with a given Id to a table for which the Id is usually generated automatically
        /// </summary>
        /// <param name="db"></param>
        /// <param name="table"></param>
        /// <returns></returns>
        public static async Task SaveChangesAsyncIdentityInsertOn(this ApplicationDbContext db, string table)
        {
            await db.Database.OpenConnectionAsync();
            try
            {
#pragma warning disable EF1002 // Risk of vulnerability to SQL injection.
                await db.Database.ExecuteSqlRawAsync($"SET IDENTITY_INSERT dbo.{table} ON");
#pragma warning restore EF1002 // Risk of vulnerability to SQL injection.
                await db.SaveChangesAsync();
            }
            catch (Exception ex)
            {
                await Task.FromException(ex);
            }
            finally
            {
#pragma warning disable EF1002 // Risk of vulnerability to SQL injection.
                await db.Database.ExecuteSqlRawAsync($"SET IDENTITY_INSERT dbo.{table} OFF");
#pragma warning restore EF1002 // Risk of vulnerability to SQL injection.
                await db.Database.CloseConnectionAsync();
            }
        }
        /// <summary>
        /// Determine the name of the entity database table
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="db"></param>
        /// <returns></returns>
        public static string TableOfDbSet<T>(this DbContext context) where T : class
        {
            string? Table = (context.EntityType(typeof(T))?.GetTableName()) ??
                throw new InvalidOperationException($"The table for type {typeof(T).Name} is not found for DbContext");
            return Table;
        }
        /// <summary>
        /// Determine the name of the entity database table
        /// </summary>
        /// <param name="context"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public static string? TableOfDbSet(this DbContext context, Type type) => context.EntityType(type)?.GetTableName() ??
                throw new InvalidOperationException($"The table for type {type.Name} is not found for DbContext");
        /// <summary>
        /// Accessing a table by entity type as a parameter. https://stackoverflow.com/questions/21533506/find-a-specified-generic-dbset-in-a-dbcontext-dynamically-when-i-have-an-entity
        /// </summary>
        /// <param name="context"></param>
        /// <param name="entity"></param>
        /// <returns></returns>
        public static IQueryable<object> Set(this DbContext context, Type entity) =>
            context.ClrQuery(context.ClrType(entity));
        /// <summary>
        /// Accessing a table by CLR type as a parameter. https://stackoverflow.com/questions/21533506/find-a-specified-generic-dbset-in-a-dbcontext-dynamically-when-i-have-an-entity
        /// </summary>
        /// <param name="context"></param>
        /// <param name="entityType"></param>
        /// <returns></returns>
        public static IQueryable<object> ClrQuery(this DbContext context, Type? entityType)
        {
            if (entityType == null)
                throw new InvalidOperationException("Entity type could not be null");
            MethodInfo? method = typeof(DbContext).GetMethods()
                .Where(p => p.Name == nameof(DbContext.Set) && p.ContainsGenericParameters).FirstOrDefault() ?? throw new InvalidOperationException("Method Set is not found for DbContext");
            method = method.MakeGenericMethod(entityType);
            object? Query = method.Invoke(context, null);
            if (Query as IQueryable<object> == null)
                throw new InvalidOperationException($"The method invoke could not return null");
            return (Query as IQueryable<object>)!;
        }
        /// <summary>
        /// Get entity type by table name
        /// </summary>
        /// <param name="context"></param>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public static Type? GetEntityTypeFromTableName(this DbContext context, string tableName) =>
            context.Model.GetEntityTypes().Select(entityType => { 
                string? mappedTableName = entityType.GetTableName();
                if (mappedTableName != null && mappedTableName.Equals(tableName, StringComparison.OrdinalIgnoreCase))
                    return entityType.ClrType;
                else return null;
            }).FirstOrDefault(r => r != null);
        /// <summary>
        /// Get all properties of a context that table contains
        /// </summary>
        /// <param name="context"></param>
        /// <returns></returns>
        public static List<PropertyInfo> GetDbSetProperties(this DbContext context)
        {
            List<PropertyInfo> dbSetProperties = [];
            PropertyInfo[] properties = context.GetType().GetProperties();
            foreach (PropertyInfo property in properties)
            {
                Type setType = property.PropertyType;
                bool isDbSet = setType.IsGenericType && typeof(DbSet<>).IsAssignableFrom(setType.GetGenericTypeDefinition());
                if (isDbSet && context.GetEntityTypeFromTableName(property.Name) != null) 
                    dbSetProperties.Add(property);
            }
            return dbSetProperties;
        }
        /// <summary>
        /// Defining the CLR type of a database entity for primary key lookups and other model accesses
        /// </summary>
        /// <param name="context"></param>
        /// <param name="entity"></param>
        /// <returns></returns>
        public static Type? ClrType(this DbContext context, Type entity) => context.Model.FindEntityType(entity)?.ClrType;
        /// <summary>
        /// Defining the RuntimeEntityType of a database entity for primary key lookups and other model accesses
        /// </summary>
        /// <param name="context"></param>
        /// <param name="entity"></param>
        /// <returns></returns>
        public static RuntimeEntityType? EntityType(this DbContext context, Type entity) =>
            (RuntimeEntityType?)context.Model.FindEntityType(entity);
        /// <summary>
        /// Defining the RuntimeEntityType of a database entity for primary key lookups and other model accesses
        /// </summary>
        /// <param name="context"></param>
        /// <param name="entity"></param>
        /// <returns></returns>
        public static RuntimeEntityType? EntityType(this DbContext context, string entity) => 
            (RuntimeEntityType?)context.Model.FindEntityType(entity);
        /// <summary>
        /// Search for a type in the database by its name
        /// </summary>
        /// <param name="context"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public static Type GetType(this DbContext context, string type)
        {
            Type? Entity = context.GetType().Assembly.GetExportedTypes()
                .FirstOrDefault(t => t.Name.Equals(type, StringComparison.OrdinalIgnoreCase)) ??
                throw new InvalidOperationException($"The type {type} was not found in DbContext");
            return Entity;
        }
        /// <summary>
        /// Updating class properties from another identical class. Throws an error if the classes are different.
        /// Returns a list of changed properties. Only properties that are not references or collections are updated.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="entity"></param>
        /// <param name="source"></param>
        public static Collection<PropertyInfo> UpdateDbPropertiesFrom<T>(this T entity, T source,
            ApplicationDbContext db, bool ExceptPrimaryKey = false) where T : class
        {
            Collection<PropertyInfo> ChangedProperties = [];
            PropertyInfo[] properties = typeof(T).GetProperties();
            IReadOnlyKey? PrimaryKey = db.PrimaryKey(typeof(T)) ?? throw new Exception("Primary key could not be null");
            foreach (PropertyInfo property in properties)
            {
                if (!ExceptPrimaryKey || !PrimaryKey.Properties.Any(r => r.Name == property.Name))
                {
                    PropertyEntry? member = db.Entry(entity).Properties.FirstOrDefault(r => r.Metadata.Name == property.Name);
                    if (member != null && member.Metadata.FindAnnotation("NotMapped") == null &&
                        !db.Entry(entity).Navigations.Any(r => r.Metadata.Name == property.Name))
                    {
                        string? EntityValue = property.GetValue(entity)?.ToString();
                        string? SourceValue = property.GetValue(source)?.ToString();
                        if (EntityValue != SourceValue)
                        {
                            property.SetValue(entity, property.GetValue(source));
                            ChangedProperties.Add(property);
                        }
                    }
                }
            }
            return ChangedProperties;
        }
        /// <summary>
        /// Determines whether the primary key for the specified entity type is configured to be generated by the database.
        /// </summary>
        /// <typeparam name="T">The entity type for which to check the primary key generation configuration.</typeparam>
        /// <param name="db">The database context used to retrieve primary key information for the entity type.</param>
        /// <returns>true if the primary key is generated by the database; otherwise, false.</returns>
        /// <exception cref="Exception">Thrown if the primary key for the specified entity type cannot be found.</exception>
        public static bool IsGeneratedKey<T>(ApplicationDbContext db)
        {
            IReadOnlyKey? PrimaryKey = db.PrimaryKey(typeof(T)) ?? throw new Exception("Primary key could not be null");
            return !PrimaryKey.Properties.Any(r => r.ValueGenerated == ValueGenerated.Never);
        }
        /// <summary>
        /// Определить первичный ключ сущности
        /// </summary>
        /// <param name="context"></param>
        /// <param name="entity"></param>
        /// <returns></returns>
#pragma warning disable EF1001 // Internal EF Core API usage.
        public static IReadOnlyKey? PrimaryKey(this DbContext context, Type entity) => 
            context.EntityType(entity)?.FindDeclaredPrimaryKey();
        /// <summary>
        /// Primary key value set
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="db"></param>
        /// <param name="entity"></param>
        /// <returns></returns>
        public static object?[] PrimaryValues<T>(this ApplicationDbContext db, T entity)
        {
            IReadOnlyKey? PrimaryKey = db.PrimaryKey(typeof(T)) ?? throw new Exception("Primary key could not be null");
            return [.. entity.GetPropertyValues([.. PrimaryKey.Properties.Select(p => p.Name)])];
        }
        /// <summary>
        /// Detaching all tracked db objects of a given type
        /// </summary>
        /// <param name="context"></param>
        public static void DetachAllEntries(this DbContext context, Type? type = null,
            EntityState? state = null, string? PropertyName = null, object? PropertyValue = null)
        {
            // var a = context.ChangeTracker.Entries(); // to check what is being disabled
            IEnumerable<EntityEntry> entries = context.ChangeTracker.Entries().Where(r =>
                (type == null || r.Entity.GetType() == type) &&
                (PropertyName == null || r.Entity.GetPropertyValue(PropertyName)?.ToString() == PropertyValue?.ToString()) &&
                (state == null || context.Entry(r.Entity).State == state));
            foreach (EntityEntry entry in entries)
            {
                context.Entry(entry.Entity).State = EntityState.Detached;
            }
        }
        /// <summary>
        /// Detach a single tracked db object of a given type with a given list of primary keys
        /// </summary>
        /// <param name="context"></param>
        public static void DetachEntry(this DbContext context, Type type, params object[] KeyValues)
        {
            // var a = context.ChangeTracker.Entries(); // to check what is being disabled
            List<EntityEntry> entries = [.. context.ChangeTracker.Entries().Where(r => r.Entity.GetType() == type)];
            IReadOnlyKey? PrimaryKey = context.PrimaryKey(type) ?? throw new Exception($"Type {type} has not primary key");
            foreach (EntityEntry entry in entries)
            {
                for (int i = 0; i < PrimaryKey.Properties.Count; i++)
                {
                    if (entry.Entity.GetPropertyValue(PrimaryKey.Properties[i].Name)?.ToString() != KeyValues[i]?.ToString())
                        goto nextEntry;
                }
                context.Entry(entry.Entity).State = EntityState.Detached;
                return;
            nextEntry:;
            }
        }
        /// <summary>
        /// Displaying all monitored objects in a table, used in case of an error
        /// </summary>
        /// <param name="context"></param>
        public static string ShowEntries(this DbContext context)
        {
            string Report = "<table><tr><td></td></tr>";
            List<EntityEntry> entries = [.. context.ChangeTracker.Entries().OrderBy(r => r.Entity.GetType()).ThenBy(r => r.State)];
            foreach (EntityEntry entry in entries)
            { // make the table here
                //Report = 
                //context.Entry(entry.Entity).State = EntityState.Detached;
            }
            Report = "</table>";
            return Report;
        }
        /// <summary>
        /// Show attached on console
        /// </summary>
        /// <param name="entries"></param>
        public static void DisplayStates(this ChangeTracker Tracker)
        {
            foreach (EntityEntry entry in Tracker.Entries())
            {
                Console.WriteLine($"Entity: {entry.Entity.GetType().Name}, State: {entry.State}");
            }
        }
#pragma warning restore EF1001 // Internal EF Core API usage.
        /// <summary>
        /// The next numeric index for the given table and key field
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="entity"></param>
        /// <param name="propertyName"></param>
        /// <returns></returns>
        public static int NextId<T>(this IQueryable<T> Source, string propertyName, bool IsZeroAllowed = true) where T : class
        {
            if (!Source.Any())
                return IsZeroAllowed ? 0 : 1;
            else return (int)Source.Max(propertyName) + 1;
        }
        /// <summary>
        /// Updating a record with commit or rollback for a table with one or more keys
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="dbSet"></param>
        /// <param name="db"></param>
        /// <param name="Entity"></param>
        /// <returns></returns>
        public static async Task<T?> UpdateWithCommit<T>(this DbSet<T> dbSet,
            ApplicationDbContext db, T Entity, bool IsNothingToChangeError = false, CancellationToken token = default) where T : class, new()
        {
            db.Entry(Entity).State = EntityState.Detached; // detach tracking, otherwise instead of the old data from the db we will get the changed data
            object?[] KeyValues = db.PrimaryValues(Entity);
            T? dbEntity = await dbSet.FindAsync(KeyValues, token);
            if (dbEntity == null) return IsNothingToChangeError ? await Task.FromException<T>(new Exception(AppMessages.NothingChanged)) : null;
            Entity.SetEmptyStringsToNulls(true); // Spaces are considered empty
            Collection<PropertyInfo> ChangedProperties = [];
            try
            {
                ChangedProperties = dbEntity.UpdateDbPropertiesFrom(Entity, db);
            }
            catch (Exception ex)
            {
                return await Task.FromException<T>(new Exception(ex.InnerException == null ? ex.Message : ex.InnerException.Message));
            }
            if (ChangedProperties.Count == 0) return IsNothingToChangeError ?
                    await Task.FromException<T>(new Exception(AppMessages.NothingChanged)) : Entity;
            db.Entry(dbEntity).State = EntityState.Modified;
            IExecutionStrategy strategy = db.Database.CreateExecutionStrategy();
            try
            { // attempting to execute transaction elements asynchronously yields the error there are pending elements...
                strategy.ExecuteInTransaction((db, dbSet, ChangedProperties),
                    operation: context =>
                    {
                        try
                        {
                            context.db?.SaveChanges(acceptAllChangesOnSuccess: false);
                        }
                        catch (DbUpdateConcurrencyException ex)
                        {
                            if (!(ex.Entries.Count == 1 && ChangedProperties.Select(r => r.Name).Contains(nameof(ApplicationUser.LastTime))))
                            { // we do not consider a simple rewrite of the last visit time to be an error and do not process it
                                foreach (EntityEntry entry in ex.Entries)
                                {
                                    //if (entry.Entity is ApplicationUser)
                                    //{
                                    //    var proposedValues = entry.CurrentValues;
                                    //    var databaseValues = entry.GetDatabaseValues();

                                    //    foreach (var property in proposedValues.Properties)
                                    //    {
                                    //        var proposedValue = proposedValues[property];
                                    //        var databaseValue = databaseValues[property];
                                    //        // TODO: decide which value should be written to database
                                    //        // proposedValues[property] = <value to be saved>;
                                    //    }
                                    //    // Refresh original values to bypass next concurrency check
                                    //    entry.OriginalValues.SetValues(databaseValues);
                                    //}
                                    //else
                                    //{
                                    //    throw new NotSupportedException(
                                    //        "Don't know how to handle concurrency conflicts for "
                                    //        + entry.Metadata.Name);
                                    //}
                                }
                            }
                        }
                    },
                    verifySucceeded: context => context.dbSet.Find(KeyValues).PropertiesEqual(dbEntity, ChangedProperties));
            }
            catch (RetryLimitExceededException ex)
            {
                db.Entry(dbEntity).State = EntityState.Detached;
                return await Task.FromException<T>(new Exception(ex.InnerException == null ? ex.Message : ex.InnerException.Message));
            }
            catch (Exception ex)
            {
                db.Entry(dbEntity).State = EntityState.Detached;
                return await Task.FromException<T>(new Exception(ex.InnerException == null ? ex.Message : ex.InnerException.Message));
            }
            db.ChangeTracker.AcceptAllChanges();
            Entity.UpdateDbPropertiesFrom(dbEntity, db);
            return Entity;
        }
        /// <summary>
        /// Selecting a property from the attached context or searching the database without calculations, for string substitutions CultureManager.GetContentArgument
        /// </summary>
        /// <param name="db"></param>
        /// <param name="name"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public static async Task<string?> GetValueAsync<T>(this ApplicationDbContext db, string name, T context, bool isThis = false, CancellationToken token = default)
        {
            // From the context included:
            // this.Property - property selection +
            // this.Category, this.Brand, this.Parent - included entity 1=>1 or +
            // this.Parameter(keyValue || idValue) - included list element by id or key -
            // From the mentioned db table:
            // Entity(value...) - entity by primary key or "id", or "key" property -
            // Entity(Prop1==value1&&Prop2==value2...).Property - selection of propety value from table Entity with field Key which equal Value -
            if (string.IsNullOrEmpty(name)) return string.Empty;
            if (name.StartsWith("this.", StringComparison.InvariantCultureIgnoreCase) && context != null) // selection from context
            {
                object? value = context.GetEntityValue(name[(name.IndexOf(".", StringComparison.InvariantCultureIgnoreCase) + 1)..], out _);
                return value?.ToString();
            }
            else if (isThis && context != null) // selection from context
            {
                object? value = context.GetEntityValue(name, out _);
                return value?.ToString();
            }
            else if (name.IndexOf("(", StringComparison.InvariantCultureIgnoreCase) > 0 &&
                name.IndexOf(")", StringComparison.InvariantCultureIgnoreCase) > name.IndexOf("(", StringComparison.InvariantCultureIgnoreCase))
            {
                string typename = name[0..name.IndexOf("(", StringComparison.InvariantCultureIgnoreCase)];
                Type entityType = db.GetType(typename) ?? throw new Exception($"Database does not contain entity {typename}.");
                string arg = name[(name.IndexOf("(", StringComparison.InvariantCultureIgnoreCase) + 1)..name.IndexOf(")", StringComparison.InvariantCultureIgnoreCase)];
                object? value = await db.GetDbValue(entityType, arg, token);
                string next = name[(name.IndexOf(")", StringComparison.InvariantCultureIgnoreCase) + 1)..];
                // If there is no next operation on the context and the property is not specified, return value
                if (string.IsNullOrEmpty(next) || !next.StartsWith(".", StringComparison.InvariantCultureIgnoreCase))
                {
                    object? result = value.GetPropertyValue(Consts.Value);
                    return result?.ToString();
                }
                return await GetValueAsync(db, name[(name.IndexOf(".", StringComparison.InvariantCultureIgnoreCase) + 1)..], value, true, token);
            }
            return null;
        }
        /// <summary>
        /// Selecting a value from a data table according to the given argument values
        /// </summary>
        /// <param name="db"></param>
        /// <param name="entityType"></param>
        /// <param name="arg"></param>
        /// <returns></returns>
        public static async Task<object?> GetDbValue(this ApplicationDbContext? db, Type? entityType, string? arg, CancellationToken token = default)
        {
            if (db == null || entityType == null || string.IsNullOrEmpty(arg)) return null;
            // We parse using a simple syntax, valid operations are = and &; the number of argument pairs is no more than 2
            // We'll expand this later as needed
            Dictionary<string, string> args = [];
            arg = arg.Replace("==", "=", StringComparison.InvariantCultureIgnoreCase)
                .Replace(" ", string.Empty, StringComparison.InvariantCultureIgnoreCase)
                .Replace("&&", "&", StringComparison.InvariantCultureIgnoreCase);
            IReadOnlyKey? primaryKey = db.PrimaryKey(entityType);
            RuntimeProperty? IdKey = db.EntityType(entityType)?.FindProperty(Consts.Id);
            RuntimeProperty? KeyKey = db.EntityType(entityType)?.FindProperty(Consts.Key);
            RuntimeProperty? NameKey = db.EntityType(entityType)?.FindProperty("Name");
            for (int i = 0; i < arg.Split("&").Length; i++)
            {
                string[] pair = arg.Split("&")[i].Split("=");
                if (pair.Length == 1)
                    args.Add(Consts.Key, pair[0]);
                else
                    args.Add(pair[0], pair[1]);
            }
            if (arg.Contains('&', StringComparison.InvariantCultureIgnoreCase)) // two keys // Entity(key=value ...).Property - selecting the value of the property from the entity table by keys key with values ​​value
                return await db.FindAsync(entityType, [args.ElementAt(0).Key, args.ElementAt(1).Key], token);
            else
            { // Entity(value...) - selection by primary key or by property named id, key or name
                IQueryable? query = db.Set(entityType)
                    .Where(args.ElementAt(0).Value, entityType, typeof(string), primaryKey?.Properties[0].Name, IdKey?.Name, KeyKey?.Name, NameKey?.Name);
                IQueryable<object>? queryres = query.QueryToResult(entityType);
                object? result = queryres == null ? null : await queryres.AsNoTracking().FirstOrDefaultAsync(token);
                if (result == null && entityType == typeof(PageText)) // add a key if there are no translations
                    return await db.PageTexts.InsertWithCommit(db, new PageText { Key = args.ElementAt(0).Value, Value = args.ElementAt(0).Value }, 0, token);
                return result;
            }
        }
        /// <summary>
        /// Updating a single record property for a table with a single key
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="dbSet"></param>
        /// <param name="db"></param>
        /// <param name="Entity"></param>
        /// <returns></returns>
        public static Task<int> ExecuteUpdateAsync(this ApplicationDbContext db, Type EntityType, object KeyValue,
            string FieldName, object? FieldValue, CancellationToken token = default)
        {
            IReadOnlyKey? PrimaryKey = db.PrimaryKey(EntityType) ?? throw new Exception($"{EntityType} has not primary key.");
            if (PrimaryKey.Properties.Count > 1)
                throw new Exception($"{EntityType} has primary key with {PrimaryKey.Properties.Count} properties.");
            try
            {
                return db.Set(EntityType).Where(PrimaryKey.Properties[0].Name, KeyValue, EntityType).ExecuteUpdateAsync(EntityType, FieldName, FieldValue, token);
            }
            catch (Exception ex) 
            {
                return Task.FromResult(0);
            }
        }
        /// <summary>
        /// Updating a single record property for all records in a selection query
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="dbSet"></param>
        /// <param name="db"></param>
        /// <param name="Entity"></param>
        /// <returns></returns>
        public static Task<int> ExecuteUpdateAsync(this IQueryable Source, Type EntityType, string FieldName, object? FieldValue, CancellationToken cancellationToken = default)
        {
            MethodInfo? UpdateAsyncMethodInfo = typeof(EntityFrameworkQueryableExtensions).GetMethod(nameof(EntityFrameworkQueryableExtensions.ExecuteUpdateAsync)) ??
                throw new Exception("EntityFrameworkQueryableExtensions.ExecuteUpdateAsync method is not found");
            ParameterExpression PropertyParam = Expression.Parameter(EntityType, "p");
            MemberExpression PropertyExp = Expression.PropertyOrField(PropertyParam, FieldName);
#if NET10_0
            // https://learn.microsoft.com/en-us/ef/core/what-is-new/ef-core-10.0/whatsnew
            Type SetPropertyBuilder = typeof(UpdateSettersBuilder<>).MakeGenericType(EntityType);
            MethodInfo SetPropertyMethodInfo = (SetPropertyBuilder.GetMethods()
                .FirstOrDefault(r => r.Name == nameof(UpdateSettersBuilder<>.SetProperty) && r.GetParameters().Any(p => p.ParameterType.BaseType == typeof(object)))?
                .MakeGenericMethod(PropertyExp.Type)) ?? throw new Exception("SetPropertyBuilder.SetProperty method is not found");
            Action<object> BuilderAction = r => SetPropertyMethodInfo.Invoke(r, [Expression.Lambda(PropertyExp, PropertyParam), FieldValue]);
            return (Task<int>)UpdateAsyncMethodInfo.MakeGenericMethod(Source.ElementType).Invoke(null, [Source, BuilderAction, cancellationToken])!;
#else // NET 9.0
            Expression ValueExp = FieldValue == null ? Expression.Constant(null, PropertyExp.Type) : Expression.Convert(Expression.Constant(FieldValue), PropertyExp.Type);
            Type SetPropertyCallsEntity = typeof(SetPropertyCalls<>).MakeGenericType(EntityType); 
            ParameterExpression EntityParam = Expression.Parameter(SetPropertyCallsEntity, "r");
            // r.SetProperty(p => p.SomeField, value)
            MethodInfo? SetPropertyMethodInfo = (SetPropertyCallsEntity.GetMethods()
                .FirstOrDefault(r => r.Name == nameof(SetPropertyCalls<object>.SetProperty) && r.GetParameters().Any(p => p.ParameterType.BaseType == typeof(object)))?
                .MakeGenericMethod(PropertyExp.Type)) ?? throw new Exception("SetPropertyCalls<>.SetProperty method is not found");
            MethodCallExpression SetBody = Expression.Call(EntityParam, SetPropertyMethodInfo, Expression.Lambda(PropertyExp, PropertyParam), ValueExp);
            LambdaExpression UpdateBody = Expression.Lambda(SetBody, EntityParam);
            return (Task<int>)UpdateAsyncMethodInfo.MakeGenericMethod(Source.ElementType).Invoke(null, [Source, UpdateBody, cancellationToken])!;
#endif
        }
        /// <summary>
        /// Updating multiple properties for all records in a selection query
        /// .ExecuteUpdateAsync(r => r.SetProperty(p => p.Status, ChatStatus.Answered).SetProperty(p => p.Big, ChatStatus.Big)...)
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="dbSet"></param>
        /// <param name="db"></param>
        /// <param name="Entity"></param>
        /// <returns></returns>
        public static Task<int> ExecuteUpdateAsync(this IQueryable Source, Type EntityType, Dictionary<string, object?> FieldValues, CancellationToken cancellationToken = default)
        {
            MethodInfo? UpdateAsyncMethodInfo = typeof(EntityFrameworkQueryableExtensions).GetMethod(nameof(EntityFrameworkQueryableExtensions.ExecuteUpdateAsync)) ??
                throw new Exception("EntityFrameworkQueryableExtensions.ExecuteUpdateAsync method is not found");
            ParameterExpression PropertyParam = Expression.Parameter(EntityType, "p");
#if NET10_0
            // https://learn.microsoft.com/en-us/ef/core/what-is-new/ef-core-10.0/whatsnew
            Type SetPropertyBuilder = typeof(UpdateSettersBuilder<>).MakeGenericType(EntityType);
            Action<object> BuilderAction = r => { };
            foreach (KeyValuePair<string, object?> FieldValue in FieldValues)
            {
                MemberExpression PropertyExp = Expression.PropertyOrField(PropertyParam, FieldValue.Key);
                MethodInfo? SetPropertyMethodInfo = (SetPropertyBuilder.GetMethods()
                    .FirstOrDefault(r => r.Name == nameof(UpdateSettersBuilder<>.SetProperty) && r.GetParameters().Any(p => p.ParameterType.BaseType == typeof(object)))?
                    .MakeGenericMethod(PropertyExp.Type)) ?? throw new Exception("SetPropertyBuilder.SetProperty method is not found");
                BuilderAction += r => SetPropertyMethodInfo.Invoke(r, [Expression.Lambda(PropertyExp, PropertyParam), FieldValue.Value]);
            }
            return (Task<int>)UpdateAsyncMethodInfo.MakeGenericMethod(Source.ElementType).Invoke(null, [Source, BuilderAction, cancellationToken])!;
#else // NET 9.0
            Type SetPropertyCallsEntity = typeof(SetPropertyCalls<>).MakeGenericType(EntityType);
            ParameterExpression EntityParam = Expression.Parameter(SetPropertyCallsEntity, "r");
            Expression SetBody = EntityParam;
            foreach (KeyValuePair<string, object?> FieldValue in FieldValues)
            {
                MemberExpression PropertyExp = Expression.PropertyOrField(PropertyParam, FieldValue.Key);
                Expression ValueExp = FieldValue.Value == null ? Expression.Constant(null, PropertyExp.Type) : Expression.Convert(Expression.Constant(FieldValue.Value), PropertyExp.Type);
                // r.SetProperty(p => p.SomeField, value).SetProperty(p => p.AnotherField, AnotherValue)
                MethodInfo? SetPropertyMethodInfo = (SetPropertyCallsEntity.GetMethods()
                    .FirstOrDefault(r => r.Name == nameof(SetPropertyCalls<object>.SetProperty) && r.GetParameters().Any(p => p.ParameterType.BaseType == typeof(object)))?
                    .MakeGenericMethod(PropertyExp.Type)) ?? throw new Exception("SetPropertyCalls<>.SetProperty method is not found");
                SetBody = Expression.Call(SetBody, SetPropertyMethodInfo, Expression.Lambda(PropertyExp, PropertyParam), ValueExp);
            }
            LambdaExpression UpdateBody = Expression.Lambda(SetBody, EntityParam);
            return (Task<int>)UpdateAsyncMethodInfo.MakeGenericMethod(Source.ElementType).Invoke(null, [Source, UpdateBody, cancellationToken])!;
#endif
        }
        /// <summary>
        /// Converting a query to a result list of some type
        /// </summary>
        /// <param name="type">desired result type</typeparam>
        /// <param name="source"></param>
        /// <returns></returns>
        public static IQueryable<object>? QueryToResult(this IQueryable source, Type type, bool IncludeAll = false)
        {
            IEnumerable<PropertyInfo> resultProperties = IncludeAll ? type.GetProperties().Where(p => p.CanWrite) :
                type.GetProperties(BindingFlags.IgnoreCase | BindingFlags.Public | BindingFlags.Instance)
                .Where(p => p.CanWrite && (p.PropertyType.IsValueType || p.PropertyType == typeof(string)));
            ParameterExpression r = Expression.Parameter(source.ElementType, "r");
            IEnumerable<MemberBinding> memberBindings = resultProperties.Select(p =>
                    Expression.Bind(type.GetMember(p.Name)[0], Expression.Property(r, p.Name))).OfType<MemberBinding>();
            Expression memberInit = Expression.MemberInit(Expression.New(type), memberBindings);
            LambdaExpression memberInitLambda = Expression.Lambda(memberInit, r);
            Type[] typeArgs = [source.ElementType, memberInit.Type];
            MethodCallExpression mc = Expression.Call(typeof(Queryable), "Select", typeArgs, source.Expression, memberInitLambda);
            IQueryable query = source.Provider.CreateQuery(mc);
            return query as IQueryable<object>;
        }
        /// <summary>
        /// Access with check to a given property
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="param"></param>
        /// <param name="propertyName"></param>
        /// <returns></returns>
        public static MemberExpression MemberExpression<T>(Expression param, string propertyName) =>
            MemberExpression(param, propertyName, typeof(T));
        /// <summary>
        /// Access with check to a given property
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="param"></param>
        /// <param name="propertyName"></param>
        /// <returns></returns>
        public static MemberExpression MemberExpression(Expression param, string propertyName, Type type)
        {
            if (string.IsNullOrEmpty(propertyName))
                throw new Exception($"Property {propertyName} does not exist"); // null if there is no such property, and there is no nested one either
            if (!propertyName.Contains('.', StringComparison.OrdinalIgnoreCase))
            {
                PropertyInfo? property = type.GetProperty(propertyName, BindingFlags.IgnoreCase | BindingFlags.Public | BindingFlags.Instance);
                if (property == null)
                {
                    PropertyInfo[] properties = type.GetProperties();
                    throw new Exception($"Property {propertyName} does not exist. There are {properties.Select(r => r.Name).JoinToString(", ")} properties only.");
                    // names such as NameJoined, NameLocalized may be present as properties of an object, but not in the table
                }
                return Expression.Property(param, property);
            }
            else
            {
                int index = propertyName.IndexOf(".", StringComparison.OrdinalIgnoreCase);
                string innerPropName = propertyName[..index];
                string restPropNames = propertyName[(index + 1)..];
                PropertyInfo? property = type.GetProperty(innerPropName, BindingFlags.IgnoreCase | BindingFlags.Public | BindingFlags.Instance);
                // error when requesting a complex property, such as Order.Time
                if (property == null)
                {
                    PropertyInfo[] properties = type.GetProperties();
                    throw new Exception($"Property {innerPropName} does not exist. There are {properties.Select(r => r.Name).JoinToString(", ")} properties only.");
                    // names such as NameJoined, NameLocalized may be present as properties of an object, but not in the table
                }
                Expression subParam = Expression.Property(param, innerPropName);
                MemberExpression? nextMember = MemberExpression(subParam, restPropNames, property.PropertyType);
                return nextMember;
            }
        }
        /// <summary>
        /// Access a nested property of any depth with null checking and type matching
        /// </summary>
        /// <param name="param"></param>
        /// <param name="propertyName"></param>
        /// <param name="type"></param>
        /// <param name="propertyType"></param>
        /// <returns></returns>
        public static Expression MemberNullCheckExpression<T>(Expression param, string propertyName) =>
            MemberNullCheckExpression(param, propertyName, typeof(T), out Type _);
        /// <summary>
        /// Access a nested property of any depth with null checking and type matching
        /// </summary>
        /// <param name="param"></param>
        /// <param name="propertyName"></param>
        /// <param name="type"></param>
        /// <param name="propertyType"></param>
        /// <returns></returns>
        public static Expression MemberNullCheckExpression(Expression param, string propertyName, Type type, out Type ResultType)
        {
            if (string.IsNullOrEmpty(propertyName)) throw new Exception($"Property {propertyName} does not exist");
            // null if there is no such property, and there is no nested one either
            if (!propertyName.Contains('.', StringComparison.OrdinalIgnoreCase))
            {
                PropertyInfo? property = type.GetProperty(propertyName, BindingFlags.IgnoreCase | BindingFlags.Public | BindingFlags.Instance) ??
                    throw new Exception($"Property {propertyName} does not exist");
                ResultType = property.PropertyType;
                return Expression.Property(param, property);
            }
            else
            {
                int index = propertyName.IndexOf(".", StringComparison.OrdinalIgnoreCase);
                string innerPropName = propertyName[..index];
                string restPropNames = propertyName[(index + 1)..];
                PropertyInfo? property = type.GetProperty(innerPropName, BindingFlags.IgnoreCase | BindingFlags.Public | BindingFlags.Instance) ??
                    throw new Exception($"Property {innerPropName} does not exist"); // error when requesting a complex property, such as Order.Time
                Expression subParam = Expression.Property(param, innerPropName);
                BinaryExpression isNull = Expression.Equal(subParam, Expression.Constant(null, property.PropertyType));
                // the object from which the next property is taken is checked for null
                Expression nextMember = MemberNullCheckExpression(subParam, restPropNames, property.PropertyType, out ResultType) ??
                    throw new Exception($"Next member does not exist");
                Expression Condition = Expression.Condition(isNull, Expression.Constant(ResultType.GetDefault(), ResultType), nextMember);
                // the next property is selected
                return Condition;
            }
        }
        /// <summary>
        /// Creating an IQueryable of a given type when the type is not known in advance
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        public static Type QueryableType(this Type type)
        {
            object? List = Activator.CreateInstance(typeof(List<>).MakeGenericType(type));
            MethodInfo? Method = typeof(Queryable).GetMethod(nameof(Queryable.AsQueryable), [typeof(IEnumerable)]) ?? throw new Exception($"Method Queryable does not exist for type {type}");
            object? ListAsQueryable = Method.Invoke(null, [List]) ?? throw new Exception($"ListAsQueryable is null");
            return ListAsQueryable.GetType();
        }
        /// <summary>
        /// Selecting and calling the Where method
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="member"></param>
        /// <param name="predicate"></param>
        /// <returns></returns>
        public static MethodCallExpression WhereCall(IQueryable queryable, LambdaExpression predicate)
        {
            MethodInfo MethodWhere = typeof(Queryable).GetMethods(BindingFlags.Static | BindingFlags.Public)
                .First(m => m.Name.Equals(nameof(Queryable.Where), StringComparison.OrdinalIgnoreCase) && m.GetParameters().Length == 2).MakeGenericMethod(queryable.ElementType);
            return Expression.Call(null, MethodWhere, queryable.Expression, Expression.Quote(predicate));
        }
        /// <summary>
        /// Selecting and calling the Where method with type conversion
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="member"></param>
        /// <param name="predicate"></param>
        /// <returns></returns>
        public static MethodCallExpression WhereCall(IQueryable queryable, LambdaExpression predicate, Type type)
        {
            MethodInfo MethodWhere = typeof(Queryable).GetMethods(BindingFlags.Static | BindingFlags.Public)
                .First(m => m.Name.Equals(nameof(Queryable.Where), StringComparison.OrdinalIgnoreCase) && m.GetParameters().Length == 2).MakeGenericMethod(type);
            MethodInfo MethodCast = typeof(Queryable).GetMethods(BindingFlags.Static | BindingFlags.Public)
                .First(m => m.Name.Equals(nameof(Queryable.Cast), StringComparison.OrdinalIgnoreCase)).MakeGenericMethod(type); // Expression.Call(null, MethodCast, queryable.Expression)
            return Expression.Call(null, MethodWhere, Expression.Call(null, MethodCast, queryable.Expression), Expression.Quote(predicate)); // Expression.Convert(queryable.Expression, type.QueryableType())
        }
        /// <summary>
        /// Selecting and calling the Count method
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="member"></param>
        /// <param name="predicate"></param>
        /// <returns></returns>
        public static MethodCallExpression CountEnumCall(Expression member, Expression boolExpression, ParameterExpression Param, Type type)
        {
            MethodInfo MethodCount = typeof(Enumerable).GetMethods()
                .First(m => m.Name.Equals(nameof(Enumerable.Count), StringComparison.OrdinalIgnoreCase) && m.GetParameters().Length == 2).MakeGenericMethod(type);
            LambdaExpression lambda = Expression.Lambda(boolExpression, Param);
            return Expression.Call(MethodCount, member, lambda);
        }
        /// <summary>
        /// Selecting and calling the GroupBy method
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="member"></param>
        /// <param name="predicate"></param>
        /// <returns></returns>
        public static MethodCallExpression GroupByCall(IQueryable queryable, LambdaExpression predicate, Type typeKey, Type typeSource)
        {
            MethodInfo MethodGroupBy = typeof(Queryable).GetMethods(BindingFlags.Static | BindingFlags.Public)
                .First(m => m.Name.Equals(nameof(Queryable.GroupBy), StringComparison.OrdinalIgnoreCase) && m.GetParameters().Length == 2).MakeGenericMethod(typeSource, typeKey);
            return Expression.Call(null, MethodGroupBy, Expression.Convert(queryable.Expression, typeSource.QueryableType()), Expression.Quote(predicate));
        }
        /// <summary>
        /// Selecting and calling the Select method
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="member"></param>
        /// <param name="predicate"></param>
        /// <returns></returns>
        public static MethodCallExpression SelectCall(IQueryable queryable, LambdaExpression predicate, Type typeKey, Type typeSource)
        {
            MethodInfo MethodSelect = typeof(Queryable).GetMethods(BindingFlags.Static | BindingFlags.Public)
                .First(m => m.Name.Equals(nameof(Queryable.Select), StringComparison.OrdinalIgnoreCase) && m.GetParameters().Length == 2).MakeGenericMethod(typeSource, typeKey);
            return Expression.Call(null, MethodSelect, queryable.Expression, Expression.Quote(predicate));
        }
        /// <summary>
        /// Selecting and calling the Any method
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="member"></param>
        /// <param name="predicate"></param>
        /// <returns></returns>
        public static MethodCallExpression AnyCall<T>(MemberExpression member, Expression predicate)
        {
            return AnyCall(member, predicate, typeof(T));
        }
        /// <summary>
        /// Selecting and calling the Any method
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="member"></param>
        /// <param name="predicate"></param>
        /// <returns></returns>
        public static MethodCallExpression AnyCall(MemberExpression member, Expression predicate, Type type)
        {
            MethodInfo MethodAny = typeof(Enumerable).GetMethods(BindingFlags.Static | BindingFlags.Public)
                .First(m => m.Name.Equals(nameof(Enumerable.Any), StringComparison.OrdinalIgnoreCase) && m.GetParameters().Length == 2).MakeGenericMethod(type);
            MethodCallExpression Call = Expression.Call(MethodAny, member, predicate);
            return Call;
        }
        /// <summary>
        /// Selecting and calling a method Ef Like
        /// </summary>
        /// <param name="member"></param>
        /// <param name="predicate"></param>
        /// <returns></returns>
        public static MethodCallExpression LikeCall(Expression member, ConstantExpression pattern)
        {
            // https://stackoverflow.com/questions/44972524/iqueryable-wherelike-extension-with-unknown-property-type
            MethodInfo? methodInfo = typeof(DbFunctionsExtensions).GetMethod(nameof(DbFunctionsExtensions.Like), BindingFlags.Static | BindingFlags.Public, null,
                [typeof(DbFunctions), typeof(string), typeof(string)], null) ?? throw new Exception($"Method Like is not found");
            return Expression.Call(methodInfo, Expression.Property(null, typeof(EF), nameof(EF.Functions)), member, pattern);
        }
        /// <summary>
        /// Selecting and calling string methods Startswith, Endswith, Contains
        /// </summary>
        /// <param name="member"></param>
        /// <param name="predicate"></param>
        /// <returns></returns>
        public static MethodCallExpression StringMethodCall(Expression member, ConstantExpression search, string method)
        {
            MethodInfo methodInfo = typeof(string).GetMethods().First(m => m.Name.Equals(method, StringComparison.OrdinalIgnoreCase) && m.GetParameters().Length == 2);
            Expression Comparison = Expression.Constant(StringComparison.OrdinalIgnoreCase, typeof(StringComparison));
            return Expression.Call(member, methodInfo, search, Comparison);
        }
        /// <summary>
        /// Selecting and calling a method OrderBy
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="member"></param>
        /// <param name="predicate"></param>
        /// <returns></returns>
        public static MethodCallExpression OrderByCall<T>(IQueryable<T> queryable, LambdaExpression predicate, Type PropertyType, string method = "OrderBy")
        {
            MethodInfo MethodOrderBy = typeof(Queryable).GetMethods(BindingFlags.Static | BindingFlags.Public)
                .First(m => m.Name.Equals(method, StringComparison.OrdinalIgnoreCase) && m.GetParameters().Length == 2).MakeGenericMethod(typeof(T), PropertyType);
            return Expression.Call(null, MethodOrderBy, queryable.Expression, Expression.Quote(predicate));
        }
        /// <summary>
        /// Selecting and calling a method OrderBy with a type conversion
        /// </summary>
        /// <param name="queryable"></param>
        /// <param name="predicate"></param>
        /// <param name="EntityType"></param>
        /// <param name="PropertyType"></param>
        /// <param name="method"></param>
        /// <returns></returns>
        public static MethodCallExpression OrderByCall(IQueryable<object> queryable, LambdaExpression predicate, Type type, Type PropertyType, string method = "OrderBy")
        {
            MethodInfo MethodOrderBy = typeof(Queryable).GetMethods(BindingFlags.Static | BindingFlags.Public)
                .First(m => m.Name.Equals(method, StringComparison.OrdinalIgnoreCase) && m.GetParameters().Length == 2).MakeGenericMethod(type, PropertyType);
            MethodInfo MethodCast = typeof(Queryable).GetMethods(BindingFlags.Static | BindingFlags.Public)
                .First(m => m.Name.Equals(nameof(Queryable.Cast), StringComparison.OrdinalIgnoreCase)).MakeGenericMethod(type);
            return Expression.Call(null, MethodOrderBy, Expression.Call(null, MethodCast, queryable.Expression), Expression.Quote(predicate)); // Expression.Convert(queryable.Expression, type.QueryableType())
        }
        /// <summary>
        /// Selecting and calling a method FirstOrDefault
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="member"></param>
        /// <param name="predicate"></param>
        /// <returns></returns>
        public static MethodCallExpression FirstOrDefaultCall<T>(MemberExpression member, LambdaExpression predicate) =>
            FirstOrDefaultCall(member, predicate, typeof(T));
        /// <summary>
        /// Selecting and calling a method FirstOrDefault
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="member"></param>
        /// <param name="predicate"></param>
        /// <returns></returns>
        public static MethodCallExpression FirstOrDefaultCall(MemberExpression member, LambdaExpression predicate, Type type)
        {
            // Starting with NET 6, there are two very similar FirstOrDefault functions. The first has a TSource second parameter, while we need the one with a Func parameter.
            // You can't use the three-parameter version, as it doesn't translate into LINQ expressions. We search by base type. The quoted fragment is used for searching.
            //var MethodFirstOrDefaults = typeof(Enumerable).GetMethods(BindingFlags.Static | BindingFlags.Public)
            //   .Where(m => m.Name.Equals(nameof(Enumerable.FirstOrDefault), StringComparison.OrdinalIgnoreCase) && m.GetParameters().Length == 2).ToList();
            //foreach (MethodInfo item in MethodFirstOrDefaults)
            //{
            //    var pars = item.GetParameters();
            //    Type type4 = pars[0].ParameterType;
            //    Type type1 = pars[1].ParameterType;
            //    Type type2 = pars[0].ParameterType.BaseType;
            //    Type type3 = pars[1].ParameterType.BaseType;
            //}
            MethodInfo MethodFirstOrDefault = typeof(Enumerable).GetMethods(BindingFlags.Static | BindingFlags.Public)
                .First(m => m.Name.Equals(nameof(Enumerable.FirstOrDefault), StringComparison.OrdinalIgnoreCase) && m.GetParameters().Length == 2 &&
                    m.GetParameters().Any(p => p.ParameterType.BaseType?.Name == nameof(MulticastDelegate))).MakeGenericMethod(type);
            return Expression.Call(MethodFirstOrDefault, member, predicate);
        }
        /// <summary>
        /// Selecting and calling the method for adding two strings
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="member"></param>
        /// <param name="predicate"></param>
        /// <returns></returns>
        public static MethodCallExpression StringConcat(Expression member1, Expression member2)
        {
            MethodInfo? ConcatMethod = typeof(string).GetMethod(nameof(string.Concat), [typeof(string), typeof(string)]) ?? throw new Exception($"Method Concat is not found");
            return Expression.Call(ConcatMethod, member1, member2);
        }
        /// <summary>
        /// Retrieving the value of a dictionary element by key
        /// </summary>
        /// <param name="parameter"></param>
        /// <param name="Key"></param>
        /// <returns></returns>
        public static MethodCallExpression DictionaryItemValue(ParameterExpression member, string key)
        {
            MethodInfo ItemMethod = typeof(IDictionary<string, object?>).GetMethods()
                    .First(m => m.Name.Equals("get_Item", StringComparison.OrdinalIgnoreCase));
            ConstantExpression KeyConstant = Expression.Constant(key, typeof(string));
            return Expression.Call(member, ItemMethod, KeyConstant);
        }
        /// <summary>
        /// Extension for filtering by property name and value
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="source"></param>
        /// <param name="property"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public static IQueryable<TEntity> Where<TEntity, TValue>(this IQueryable<TEntity> source, string equalProperty, TValue? value)
            where TEntity : class
        {
            Type type = typeof(TEntity);
            PropertyInfo? property = type.GetProperty(equalProperty) ?? throw new NotImplementedException($"Type {type.Name} does not contain property {equalProperty}");
            ParameterExpression parameter = Expression.Parameter(type, "r");
            MemberExpression member = Expression.MakeMemberAccess(parameter, property);
            LambdaExpression whereExpression = Expression.Lambda(Expression.Equal(member, Expression.Constant(value, property.PropertyType)), parameter);
            MethodCallExpression resultExpression = WhereCall(source, whereExpression);
            return source.Provider.CreateQuery<TEntity>(resultExpression);
        }
        /// <summary>
        /// Extension for filtering by property name and value
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="source"></param>
        /// <param name="property"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public static IQueryable Where(this IQueryable source, string equalProperty, object? value, Type EntityType)
        {
            PropertyInfo? property = EntityType.GetProperty(equalProperty) ?? throw new NotImplementedException($"Type {EntityType.Name} does not contain property {equalProperty}");
            ParameterExpression parameter = Expression.Parameter(EntityType, "r");
            MemberExpression member = Expression.MakeMemberAccess(parameter, property);
            LambdaExpression whereExpression = Expression.Lambda(Expression.Equal(member, Expression.Constant(value, property.PropertyType)), parameter);
            MethodCallExpression resultExpression = WhereCall(source, whereExpression);
            return source.Provider.CreateQuery(resultExpression);
        }
        /// <summary>
        /// Extension for filtering by value and property names, one of the properties must be equal
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="source"></param>
        /// <param name="property"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public static IQueryable Where(this IQueryable source, object? value, Type EntityType, Type ValueType, params string?[] FieldNames)
        {
            ParameterExpression parameter = Expression.Parameter(EntityType, "r");
            Expression body = Expression.Constant(false);
            List<string> Props = [];
            foreach (string? FieldName in FieldNames.Where(r => !string.IsNullOrEmpty(r)).Distinct())
            {
                PropertyInfo? property = string.IsNullOrEmpty(FieldName) ? null : EntityType.GetProperty(FieldName);
                if (property != null && property.PropertyType == ValueType)
                {
                    MemberExpression member = Expression.MakeMemberAccess(parameter, property);
                    body = Expression.OrElse(body, Expression.Equal(member, Expression.Constant(value, ValueType)));
                }
            }
            LambdaExpression whereExpression = Expression.Lambda(body, parameter);
            MethodCallExpression resultExpression = WhereCall(source, whereExpression);
            return source.Provider.CreateQuery(resultExpression);
        }
        /// <summary>
        /// Does the field value match one of the list of patterns r => r.Name.Like("%[%]%")
        /// </summary>
        /// <param name="cultureField"></param>
        /// <returns></returns>
        public static Expression LikeAnyBody<T>(this Expression parameter, string field, string[] Patterns)
        {
            MemberExpression member = MemberExpression<T>(parameter, field);
            Expression body = Expression.Constant(false);
            foreach (string pattern in Patterns)
            {
                MethodCallExpression Like = LikeCall(member, Expression.Constant("%" + pattern + "%"));
                body = Expression.OrElse(body, Like);
            }
            return body;
        }
        /// <summary>
        /// Selecting and calling the Contains method for lists
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="member"></param>
        /// <param name="predicate"></param>
        /// <returns></returns>
        public static Expression ContainedInEnumerable<T>(Expression member, IEnumerable<T> enumerable) =>
            ContainedInEnumerable(member, enumerable, typeof(T));
        /// <summary>
        /// Selecting and calling the Contains method for lists has been verified to work for at least 800 items in a list when the Contains option is selected.
        /// ValueType must not be Nullable, otherwise an error occurs. Therefore, the type must be taken from an enumeration, not from a property that can be Nullable.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="member"></param>
        /// <param name="predicate"></param>
        /// <returns></returns>
        public static Expression ContainedInEnumerable(Expression member, IEnumerable ENumerable, Type ValueType)
        {
            // Solution with list search and, as an option, with search in a list of expressions
            // Option with direct list search
            // https://stackoverflow.com/questions/46203215/c-sharp-reflection-makegenerictype-for-recursive-type-parameters
            Type EnumType = ReflectionHelper.MakeGenericType(typeof(IEnumerable<>), ValueType);
            MethodInfo MethodCast = typeof(Enumerable).GetMethods(BindingFlags.Static | BindingFlags.Public)
                .First(m => m.Name.Equals(nameof(Enumerable.Cast), StringComparison.OrdinalIgnoreCase) && m.GetParameters().Length == 1).MakeGenericMethod(ValueType);
            object? TypedEnum = MethodCast.Invoke(null, [ENumerable.Cast<object>().AsEnumerable().Select(r => Convert.ChangeType(r, ValueType))]);
            Expression EnumExp = Expression.Constant(TypedEnum, EnumType);

            // Variant with conversion to a list of expressions, does not work for large lists https://stackoverflow.com/questions/31404913/expression-call-with-ienumerablestring-parameter
            //List<ConstantExpression> Exps = ((IEnumerable<object>)enumerable).Select(r => Expression.Constant(Convert.ChangeType(r, ValueType), ValueType)).ToList();
            //Expression EnumExp = Expression.NewArrayInit(ValueType, Exps);

            MethodInfo MethodContains = typeof(Enumerable).GetMethods(BindingFlags.Static | BindingFlags.Public)
                .First(m => m.Name.Equals(nameof(Enumerable.Contains), StringComparison.OrdinalIgnoreCase) && m.GetParameters().Length == 2).MakeGenericMethod(ValueType);
            return Expression.Call(null, MethodContains, EnumExp, member);

            // The solution with enumerated comparisons only works for the smallest lists
            //Expression body = Expression.Constant(false);
            //foreach (var item in enumerable)
            //{
            //    ConstantExpression elem = Expression.Constant(item, ValueType);
            //    body = Expression.OrElse(body, Expression.Equal(member, elem));
            //}
            //return body;
        }
        /// <summary>
        /// Lambda expression to determine if a given field is a member of a given list r => enumerable.Contains(r.field)
        /// </summary>
        /// <typeparam name="TEntity"></typeparam>
        /// <typeparam name="TKey"></typeparam>
        /// <param name="Field"></param>
        /// <param name="enumerable"></param>
        /// <returns></returns>
        public static Expression<Func<TEntity, bool>> IsContainedInEnumerable<TEntity, TKey>(string Field, IEnumerable<TKey> enumerable)
        {
            ParameterExpression parameter = Expression.Parameter(typeof(TEntity), "r");
            MemberExpression member = MemberExpression<TEntity>(parameter, Field);
            Expression ContainsCall = ContainedInEnumerable(member, enumerable); // CultureIds.Contains(c.CultureId) replaced by a list of comparisons
            return Expression.Lambda<Func<TEntity, bool>>(ContainsCall, parameter);
        }
        /// <summary>
        /// Selecting and calling a method ToString()
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="member"></param>
        /// <param name="predicate"></param>
        /// <returns></returns>
        public static Expression ToStringCall(Expression member, Type? SourceType = null)
        {
            SourceType ??= typeof(object);
            MethodCallExpression ToStringCall;
            if (SourceType == typeof(string))
                return member;
            if (SourceType == typeof(object))
            {
                MethodInfo methodInfo = typeof(object).GetMethods().First(m => m.Name.Equals(nameof(ToString), StringComparison.OrdinalIgnoreCase));
                ToStringCall = Expression.Call(member, methodInfo);
            }
            else
            {
                MethodInfo? methodInfo = SourceType.GetMethods()
                    .FirstOrDefault(m => m.Name.Equals(nameof(ToString), StringComparison.OrdinalIgnoreCase) && m.GetParameters().Length == (m.IsStatic ? 1 : 0));
                if (methodInfo != null && methodInfo.IsGenericMethodDefinition) methodInfo = methodInfo.MakeGenericMethod(SourceType);
                if (methodInfo == null)
                {
                    if (SourceType.Name.Contains("nullable", StringComparison.OrdinalIgnoreCase))
                    {
                        Type? OriginalType = SourceType.GenericTypeArguments[0] ?? throw new Exception($"Original type is not found for {SourceType.Name}");
                        methodInfo = OriginalType?.GetMethods()
                            .FirstOrDefault(m => m.Name.Equals(nameof(ToString), StringComparison.OrdinalIgnoreCase) && m.GetParameters().Length == 1)?.MakeGenericMethod(SourceType);
                        if (methodInfo == null)
                            throw new Exception($"Method ToString is not defined for type {SourceType.Name} and for type {OriginalType?.Name}");
                        ToStringCall = Expression.Call(methodInfo, Expression.Convert(member, OriginalType!));
                    }
                    else
                    {
                        // MethodInfo[] a = SourceType.GetMethods(); // for debug
                        throw new Exception($"Method ToString is not defined for type {SourceType.Name}");
                    }
                }
                ToStringCall = Expression.Call(member, methodInfo);
            }
            if (!SourceType.IsNullable()) return ToStringCall;
            else return Expression.Condition(Expression.Equal(member, Expression.Constant(null, SourceType)), Expression.Constant(null, typeof(string)), ToStringCall);
            // Expression.Coalesce(call, Expression.Constant(false, typeof(bool))); // checking for null, the same, but different
        }
        /// <summary>
        /// Does the field value match the given pattern r => !(r.Name.Like("%[%]%"))
        /// </summary>
        /// <param name="cultureField"></param>
        /// <returns></returns>
        public static Expression<Func<T, bool>> IsLikeValueExpression<T>(string field, string pattern)
        {
            ParameterExpression parameter = Expression.Parameter(typeof(T), "r");
            MemberExpression member = MemberExpression<T>(parameter, field);
            MethodCallExpression Like = LikeCall(member, Expression.Constant(pattern));
            return Expression.Lambda<Func<T, bool>>(Like, parameter);
        }
        /// <summary>
        /// Does the field value match the given pattern r => !(r.Name.Like("%[%]%"))
        /// </summary>
        /// <param name="cultureField"></param>
        /// <returns></returns>
        public static LambdaExpression IsLikeValueExpression(string field, string pattern, Type sourceType, Type targetType)
        {
            ParameterExpression parameter = Expression.Parameter(sourceType, "r");
            Expression ToType = Expression.Convert(parameter, targetType);
            MemberExpression member = MemberExpression(ToType, field, targetType);
            MethodCallExpression Like = LikeCall(member, Expression.Constant(pattern));
            return Expression.Lambda(Like, parameter);
        }
        /// <summary>
        /// Creating a search expression r => fields.Like(search) for startswith, endswith, contains, and equal operations. 
        /// % and _ substitutes are not undone, but [] are. FullText and other substitutes can be added.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name=Consts.Search></param>
        /// <param name="properties"></param>
        /// <param name="entity"></param>
        /// <returns></returns>
        public static Expression LikeOrEqualExpression(string search, string operation, Expression member, bool LikeAllowed = true)
        {
            if (member.Type != typeof(string)) member = ToStringCall(member, member.Type);
            Expression call;
            if (operation.Contains("equal", StringComparison.OrdinalIgnoreCase))
                call = operation.Contains("no", StringComparison.OrdinalIgnoreCase) ?
                    Expression.NotEqual(member, Expression.Constant(search, typeof(string))) :
                    Expression.Equal(member, Expression.Constant(search, typeof(string)));
            else if (operation.Equals("contains", StringComparison.OrdinalIgnoreCase) ||
                operation.Equals("like", StringComparison.OrdinalIgnoreCase))
                call = LikeAllowed ? LikeCall(member, Expression.Constant($"%{search.SearchPattern()}%", typeof(string))) :
                    StringMethodCall(member, Expression.Constant(search, typeof(string)), operation);
            else if (operation.Equals("StartsWith", StringComparison.OrdinalIgnoreCase))
                call = LikeAllowed ? LikeCall(member, Expression.Constant($"{search.SearchPattern()}%", typeof(string))) :
                    StringMethodCall(member, Expression.Constant(search, typeof(string)), operation);
            else if (operation.Equals("EndsWith", StringComparison.OrdinalIgnoreCase))
                call = LikeAllowed ? LikeCall(member, Expression.Constant($"%{search.SearchPattern()}", typeof(string))) :
                    StringMethodCall(member, Expression.Constant(search, typeof(string)), operation);
            else throw new NotImplementedException($"Operation {operation} is not implemented");
            return Expression.Condition(Expression.Equal(member, Expression.Constant(null, member.Type)), Expression.Constant(false, typeof(bool)), call);
        }
        /// <summary>
        /// Creating a search expression r => fields.Like(search) for the operations startswith, endswith, contains, =, <, >
        /// </summary>
        /// <param name=Consts.Search></param>
        /// <param name="operation"></param>
        /// <param name="member"></param>
        /// <param name="propertyType"></param>
        /// <returns></returns>
        public static Expression LikeOrCompareExpression(object? search, string operation, Expression member, bool LikeAllowed = true)
        {
                if (member.Type == typeof(object)) member = ToStringCall(member, typeof(object));
                if (search != null && operation.Equals(Operator.Equal.ToString(), StringComparison.OrdinalIgnoreCase) && member.Type == typeof(DateTime))
                // equality to a date is within this date
                return Expression.And(Expression.GreaterThanOrEqual(member, Expression.Constant(search.To(member.Type), member.Type)),
                        Expression.LessThanOrEqual(member, Expression.Constant(((DateTime)search.To(typeof(DateTime))).AddDays(1), member.Type)));
                else if (operation.Equals(Operator.Equal.ToString(), StringComparison.OrdinalIgnoreCase)
                    || (string.IsNullOrEmpty(search?.ToString()) && !operation.Contains("no", StringComparison.OrdinalIgnoreCase)))
                    return Expression.Equal(member, Expression.Constant(search?.To(member.Type), search == null ? typeof(object) : member.Type));
                else if (operation.Equals(Operator.NotEqual.ToString(), StringComparison.OrdinalIgnoreCase)
                    || (string.IsNullOrEmpty(search?.ToString()) && operation.Contains("no", StringComparison.OrdinalIgnoreCase)))
                {
                    if (!string.IsNullOrEmpty(search?.ToString()) && (search.ToString()!.Contains('%', StringComparison.OrdinalIgnoreCase) ||
                        search.ToString()!.Contains('_', StringComparison.OrdinalIgnoreCase))) // we consider the operation as not contains
                    return Expression.Not(LikeAllowed && !string.IsNullOrEmpty(search.ToString()) ?
                            LikeCall(member, Expression.Constant($"%{search.ToString()!.SearchPattern()}%", typeof(string))) :
                            StringMethodCall(member, Expression.Constant(search, typeof(string)), operation));
                    else return Expression.NotEqual(member, Expression.Constant(search?.To(member.Type), search == null ? typeof(object) : member.Type));
                }
                else if (operation.Equals(Operator.Contains.ToString(), StringComparison.OrdinalIgnoreCase))
                    return LikeAllowed && !string.IsNullOrEmpty(search?.ToString()) ?
                        LikeCall(member, Expression.Constant($"%{search.ToString()!.SearchPattern()}%", typeof(string))) :
                        StringMethodCall(member, Expression.Constant(search, typeof(string)), operation);
                else if (operation.Equals(Operator.StartsWith.ToString(), StringComparison.OrdinalIgnoreCase))
                    return LikeAllowed && !string.IsNullOrEmpty(search?.ToString()) ?
                        LikeCall(member, Expression.Constant($"{search.ToString()!.SearchPattern()}%", typeof(string))) :
                        StringMethodCall(member, Expression.Constant(search, typeof(string)), operation);
                else if (operation.Equals(Operator.EndsWith.ToString(), StringComparison.OrdinalIgnoreCase))
                    return LikeAllowed && !string.IsNullOrEmpty(search?.ToString()) ?
                        LikeCall(member, Expression.Constant($"%{search.ToString()!.SearchPattern()}", typeof(string))) :
                        StringMethodCall(member, Expression.Constant(search, typeof(string)), operation);
                else if (operation.Equals(Operator.GreaterThan.ToString(), StringComparison.OrdinalIgnoreCase))
                    return Expression.GreaterThan(member, Expression.Constant((search ?? member.Type.GetDefault()!).To(member.Type), member.Type));
                else if (operation.Equals(Operator.GreaterThanOrEqual.ToString(), StringComparison.OrdinalIgnoreCase))
                    return Expression.GreaterThanOrEqual(member, Expression.Constant((search ?? member.Type.GetDefault()!).To(member.Type), member.Type));
                else if (operation.Equals(Operator.LessThan.ToString(), StringComparison.OrdinalIgnoreCase))
                    return Expression.LessThan(member, Expression.Constant((search ?? member.Type.GetDefault()!).To(member.Type), member.Type));
                else if (operation.Equals(Operator.LessThanOrEqual.ToString(), StringComparison.OrdinalIgnoreCase))
                    return Expression.LessThanOrEqual(member, Expression.Constant((search ?? member.Type.GetDefault()!).To(member.Type), member.Type));
                else if (operation.Equals("notcontains", StringComparison.OrdinalIgnoreCase))
                    return Expression.Not(LikeAllowed && !string.IsNullOrEmpty(search?.ToString()) ?
                        LikeCall(member, Expression.Constant($"%{search.ToString()!.SearchPattern()}%", typeof(string))) :
                        StringMethodCall(member, Expression.Constant(search, typeof(string)), operation)); // proper names of operations are also possible notcontains
            else throw new NotImplementedException($"{operation} operation is not implemented");
        }
        /// <summary>
        /// Extension for grouping by property name, for example .GroupBy<T, int>(Consts.Id))
        /// </summary>
        /// <typeparam name="TEntity"></typeparam>
        /// <param name="source"></param>
        /// <param name="Property"></param>
        /// <returns></returns>
        public static IQueryable<IGrouping<TKey, T>> GroupBy<T, TKey>(this IQueryable<T> source, string Property)
            where T : class
        {
            Type type = typeof(T);
            PropertyInfo? property = type.GetProperty(Property);
            if (property == null)
                throw new NotImplementedException($"Type {type.Name} does not contain property {property}");
            if (property.PropertyType != typeof(TKey))
                throw new ArgumentException("Type of property should be equal TKey.");
            ParameterExpression parameter = Expression.Parameter(type, "r");
            MemberExpression member = Expression.MakeMemberAccess(parameter, property);
            LambdaExpression lambda = Expression.Lambda(member, parameter);
            MethodCallExpression resultExpression = GroupByCall(source, lambda, property.PropertyType, typeof(T));
            return source.Provider.CreateQuery<IGrouping<TKey, T>>(resultExpression);
        }
        /// <summary>
        /// Extension for grouping by property name, for example .GroupBy<T, int>(Consts.Id))
        /// </summary>
        /// <typeparam name="TEntity"></typeparam>
        /// <param name="source"></param>
        /// <param name="Property"></param>
        /// <returns></returns>
        public static IQueryable GroupBy(this IQueryable source, string Property, Type type)
        {
            PropertyInfo? property = type.GetProperty(Property) ?? throw new NotImplementedException($"Type {type.Name} does not contain property {Property}");
            ParameterExpression parameter = Expression.Parameter(type, "r");
            MemberExpression member = Expression.MakeMemberAccess(parameter, property);
            LambdaExpression lambda = Expression.Lambda(member, parameter);
            MethodCallExpression resultExpression = GroupByCall(source, lambda, property.PropertyType, type);
            return source.Provider.CreateQuery(resultExpression);
        }
        /// <summary>
        /// Extension for filtering by property name and like value
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="source"></param>
        /// <param name="property"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public static IQueryable<T> WhereLike<T>(this IQueryable<T> source, string equalProperty, string search, string Operator = "Like")
            where T : class
        {
            if (Operator == null || string.IsNullOrEmpty(search)) return source;
            Type type = typeof(T);
            PropertyInfo? property = type.GetProperty(equalProperty) ?? throw new NotImplementedException($"Type {type.Name} does not contain property {equalProperty}");
            ParameterExpression parameter = Expression.Parameter(type, "r");
            MemberExpression member = Expression.MakeMemberAccess(parameter, property);
            LambdaExpression whereExpression = Expression.Lambda(LikeOrEqualExpression(search, Operator, member), parameter);
            MethodCallExpression resultExpression = WhereCall(source, whereExpression);
            return source.Provider.CreateQuery<T>(resultExpression);
        }
        /// <summary>
        /// Extension for filtering by the presence of a property value in the list of options
        /// </summary>
        /// <typeparam name="TItem"></typeparam>
        /// <typeparam name="TValue"></typeparam>
        /// <param name="source"></param>
        /// <param name="FieldName"></param>
        /// <param name="Values"></param>
        /// <returns></returns>
        public static IQueryable<TItem> WhereContains<TItem, TValue>(this IQueryable<TItem> source, string FieldName, List<TValue> Values)
        {
            if (Values.Count == 0) return source;
            ParameterExpression parameter = Expression.Parameter(typeof(TItem), "r");
            MemberExpression member = MemberExpression<TItem>(parameter, FieldName);
            Expression ContainsCall = ContainedInEnumerable(Expression.Convert(member, typeof(TValue)), Values); // CultureIds.Contains(c.CultureId) replaced by a list of comparisons
            LambdaExpression whereExpression = Expression.Lambda(ContainsCall, parameter);
            MethodCallExpression resultExpression = WhereCall(source, whereExpression);
            return (IQueryable<TItem>)source.Provider.CreateQuery(resultExpression);
        }
        /// <summary>
        /// If it matches at least one of the pairs from the list, for Where(r => KeyValuePairs.Any(p => p.Key == r.ParameterId && p.Value == r.Value))
        /// </summary>
        /// <typeparam name="TItem"></typeparam>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TValue"></typeparam>
        /// <param name="source"></param>
        /// <param name="KeyValuePairs"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        public static IQueryable<T> WhereContainedInKeyValuePairs<T, TKey, TValue>(this IQueryable<T> source, 
            string KeyFieldName, string ValueFieldName, List<KeyValuePair<TKey, TValue>> KeyValuePairs)
        {
            ParameterExpression parameter = Expression.Parameter(typeof(ModelParameter), "r");
            Expression body = Expression.Constant(false);
            PropertyInfo? KeyProperty = typeof(T).GetProperty(KeyFieldName, BindingFlags.IgnoreCase | BindingFlags.Public | BindingFlags.Instance) ??
                throw new Exception($"Property {KeyFieldName} is not found in {nameof(T)}");
            PropertyInfo? ValueProperty = typeof(T).GetProperty(ValueFieldName, BindingFlags.IgnoreCase | BindingFlags.Public | BindingFlags.Instance) ??
                throw new Exception($"Property {ValueFieldName} is not found in {nameof(T)}");
            if (KeyProperty.PropertyType != typeof(TKey))
                throw new Exception($"The type of {KeyFieldName} is not the same as the KeyValuePair key");
            if (ValueProperty.PropertyType != typeof(TValue))
                throw new Exception($"The type of {ValueFieldName} is not the same as the KeyValuePair value");
            Expression ValueMember = Expression.Property(parameter, ValueProperty);
            Expression KeyMember = Expression.Property(parameter, KeyProperty);
            // replace Any with an enumeration of comparisons
            foreach (KeyValuePair<TKey, TValue> KeyValuePair in KeyValuePairs)
            {
                BinaryExpression PairExpression = Expression.AndAlso(Expression.Equal(KeyMember, Expression.Constant(KeyValuePair.Key)), 
                    Expression.Equal(ValueMember, Expression.Constant(KeyValuePair.Value)));
                body = Expression.OrElse(body, PairExpression);
            }
            LambdaExpression whereExpression = Expression.Lambda(body, parameter);
            MethodCallExpression resultExpression = WhereCall(source, whereExpression);
            return (IQueryable<T>)source.Provider.CreateQuery(resultExpression);
        }
        /// <summary>
        /// Extension for filtering by the presence of a property value in the list of options
        /// </summary>
        /// <typeparam name="TItem"></typeparam>
        /// <typeparam name="TValue"></typeparam>
        /// <param name="source"></param>
        /// <param name="FieldName"></param>
        /// <param name="Values"></param>
        /// <returns></returns>
        public static IQueryable WhereContains(this IQueryable source, string FieldName, IList Values, Type EntityType)
        {
            ParameterExpression parameter = Expression.Parameter(EntityType, "r");
            PropertyInfo? property = EntityType.GetProperty(FieldName, BindingFlags.IgnoreCase | BindingFlags.Public | BindingFlags.Instance) ?? 
                throw new Exception($"Property {FieldName} does not exist. There are {EntityType.GetProperties().Select(r => r.Name).JoinToString(", ")} properties only.");
            Type GenericListType = Values.GetType().IsGenericType ? Values.GetType().GenericTypeArguments[0] : 
                throw new Exception($"The type {Values.GetType()} is not generic");
            var ConvertedValues = Values;
            Type ConvertedListType = GenericListType;
            if (property.PropertyType != GenericListType)
            {
                if (!property.PropertyType.Equals(typeof(Nullable<>)))
                {
                    //    throw new Exception($"The type of property {property.PropertyType} is not the type of list {GenericListType}");
                    ConvertedValues = (Values as List<object>)!.Select(r => Convert.ChangeType(r, property.PropertyType)).ToList();
                    ConvertedListType = property.PropertyType;
                }
                else if (Nullable.GetUnderlyingType(property.PropertyType) != GenericListType)
                {
                    //    throw new Exception($"The type of property {property.PropertyType} is not the nullable type of list {GenericListType}");
                    ConvertedValues = (Values as List<object>)!.Select(r => Convert.ChangeType(r, Nullable.GetUnderlyingType(property.PropertyType)!)).ToList();
                    ConvertedListType = Nullable.GetUnderlyingType(property.PropertyType)!;
                }
            }
            MemberExpression member = MemberExpression(parameter, FieldName, EntityType);
            Expression ContainsCall = ContainedInEnumerable(member, ConvertedValues, ConvertedListType); // CultureIds.Contains(c.CultureId) заменен на перечисление сравнений
            LambdaExpression whereExpression = Expression.Lambda(ContainsCall, parameter);
            MethodCallExpression resultExpression = WhereCall(source, whereExpression, EntityType);
            return source.Provider.CreateQuery(resultExpression);
        }
        /// <summary>
        /// Extension for filtering by value and property names, one of the properties must be equal
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="source"></param>
        /// <param name="property"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public static IQueryable WhereInt(this IQueryable source, object? value, Type EntityType, params string[] FieldNames)
        {
            ParameterExpression parameter = Expression.Parameter(EntityType, "r");
            Expression body = Expression.Constant(false);
            List<string> Props = [];
            foreach (string FieldName in FieldNames.Where(r => !string.IsNullOrEmpty(r)).Distinct())
            {
                PropertyInfo? property = EntityType.GetProperty(FieldName);
                if (property != null && property.PropertyType == typeof(int?))
                {
                    MemberExpression member = Expression.MakeMemberAccess(parameter, property);
                    body = Expression.OrElse(body, Expression.Equal(member, Expression.Constant(value, typeof(int?))));
                }
            }
            LambdaExpression whereExpression = Expression.Lambda(body, parameter);
            MethodCallExpression resultExpression = WhereCall(source, whereExpression);
            return source.Provider.CreateQuery(resultExpression);
        }
        /// <summary>
        /// Extension for filtering by value and property names
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="source"></param>
        /// <param name="property"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public static IQueryable<T> WhereAny<T>(this IQueryable<T> source, string search, string Operator = "Like", params string[] FieldNames)
        {
            if (string.IsNullOrEmpty(search) || FieldNames.Length == 0) return source;
            ParameterExpression parameter = Expression.Parameter(typeof(T), "r");
            Expression body = Expression.Constant(false);
            foreach (string FieldName in FieldNames)
            {
                MemberExpression member = MemberExpression<T>(parameter, FieldName);
                body = Expression.OrElse(body, LikeOrEqualExpression(search, Operator, member));
            }
            LambdaExpression whereExpression = Expression.Lambda(body, parameter);
            MethodCallExpression resultExpression = WhereCall(source, whereExpression);
            return (IQueryable<T>)source.Provider.CreateQuery(resultExpression);
        }
        /// <summary>
        /// An extension for filtering Like by multiple property names at once, as quickly as possible
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="source"></param>
        /// <param name="property"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public static IQueryable<T> WhereAnyLike<T>(this IQueryable<T> source, string search, params string[] FieldNames)
        {
            if (string.IsNullOrEmpty(search) || FieldNames.Length == 0) return source;
            MethodInfo? methodInfo = typeof(DbFunctionsExtensions).GetMethod(nameof(DbFunctionsExtensions.Like), BindingFlags.Static | BindingFlags.Public, null,
                [typeof(DbFunctions), typeof(string), typeof(string)], null) ?? throw new Exception($"Method Like is not found");
            ParameterExpression parameter = Expression.Parameter(typeof(T), "r");
            Expression body = Expression.Constant(false);
            ConstantExpression SearchConst = Expression.Constant($"%{search.SearchPattern()}%", typeof(string));
            MemberExpression EfFuncExp = Expression.Property(null, typeof(EF), nameof(EF.Functions));
            int PropCount = 0;
            foreach (string FieldName in FieldNames)
            {
                // Вариант с допустимыми вложенными свойствами
                Expression member = MemberExpression<T>(parameter, FieldName);
                if (member.Type != typeof(string)) member = ToStringCall(member, member.Type);
                Expression call = Expression.Call(methodInfo, EfFuncExp, member, SearchConst);
                body = Expression.OrElse(body, call);
                PropCount++;
                // Вариант когда вложенные недопустимы
                //PropertyInfo? property = typeof(T).GetProperty(FieldName, BindingFlags.IgnoreCase | BindingFlags.Public | BindingFlags.Instance);
                //if (property != null)
                //{
                //    Expression member = Expression.Property(parameter, property);
                //    if (member.Type != typeof(string)) member = ToStringCall(member, member.Type);
                //    Expression call = Expression.Call(methodInfo, EfFuncExp, member, SearchConst);
                //    body = Expression.OrElse(body, call);
                //    PropCount++;
                //}
            }
            if (PropCount == 0)
                throw new Exception($"There is not any property {FieldNames.JoinToString(", ")} in {typeof(T).Name}");
            LambdaExpression whereExpression = Expression.Lambda(body, parameter);
            MethodCallExpression resultExpression = WhereCall(source, whereExpression);
            return (IQueryable<T>)source.Provider.CreateQuery(resultExpression);
        }
        /// <summary>
        /// An extension for filtering Like by multiple matching values ​​of a single property, as quickly as possible.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="source"></param>
        /// <param name="property"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public static IQueryable WhereLikeAny(this IQueryable source, Type EntityType, string FieldName, params string?[] searches)
        {
            if (string.IsNullOrEmpty(FieldName) || searches.Length == 0) return source;
            MethodInfo? methodInfo = typeof(DbFunctionsExtensions).GetMethod(nameof(DbFunctionsExtensions.Like), BindingFlags.Static | BindingFlags.Public, null,
                [typeof(DbFunctions), typeof(string), typeof(string)], null) ?? throw new Exception($"Method Like is not found");
            ParameterExpression parameter = Expression.Parameter(EntityType, "r");
            Expression body = Expression.Constant(false);
            PropertyInfo? property = EntityType.GetProperty(FieldName, BindingFlags.IgnoreCase | BindingFlags.Public | BindingFlags.Instance) ?? throw new Exception($"Property {FieldName} is not found in {EntityType.Name}");
            Expression member = Expression.Property(parameter, property);
            if (member.Type != typeof(string)) member = ToStringCall(member, member.Type);
            MemberExpression EfFuncExp = Expression.Property(null, typeof(EF), nameof(EF.Functions));
            int SearchCount = 0;
            foreach (string? search in searches)
            {
                if (!string.IsNullOrEmpty(search))
                {
                    ConstantExpression SearchConst = Expression.Constant($"%{search.SearchPattern()}%", typeof(string));
                    Expression call = Expression.Call(methodInfo, EfFuncExp, member, SearchConst);
                    body = Expression.OrElse(body, call);
                    SearchCount++;
                }
            }
            if (SearchCount == 0)
                throw new Exception($"There is not any string to search {searches.JoinToString(", ")}");
            LambdaExpression whereExpression = Expression.Lambda(body, parameter);
            MethodCallExpression resultExpression = WhereCall(source, whereExpression);
            return source.Provider.CreateQuery(resultExpression);
        }
        /// <summary>
        /// Extension for filtering Like by multiple values ​​that must be present in the value of one property in any order
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="source"></param>
        /// <param name="property"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public static IQueryable WhereLikeAll(this IQueryable source, Type EntityType, string FieldName, params string?[] searches)
        {
            if (string.IsNullOrEmpty(FieldName) || searches.Length == 0) return source;
            MethodInfo? methodInfo = typeof(DbFunctionsExtensions).GetMethod(nameof(DbFunctionsExtensions.Like), BindingFlags.Static | BindingFlags.Public, null,
                [typeof(DbFunctions), typeof(string), typeof(string)], null) ?? throw new Exception($"Method Like is not found");
            ParameterExpression parameter = Expression.Parameter(EntityType, "r");
            Expression body = Expression.Constant(true);
            PropertyInfo? property = EntityType.GetProperty(FieldName, BindingFlags.IgnoreCase | BindingFlags.Public | BindingFlags.Instance) ?? throw new Exception($"Property {FieldName} is not found in {EntityType.Name}");
            Expression member = Expression.Property(parameter, property);
            if (member.Type != typeof(string)) member = ToStringCall(member, member.Type);
            MemberExpression EfFuncExp = Expression.Property(null, typeof(EF), nameof(EF.Functions));
            int SearchCount = 0;
            foreach (string? search in searches)
            {
                if (!string.IsNullOrEmpty(search))
                {
                    ConstantExpression SearchConst = Expression.Constant($"%{search.SearchPattern()}%", typeof(string));
                    Expression call = Expression.Call(methodInfo, EfFuncExp, member, SearchConst);
                    body = Expression.AndAlso(body, call);
                    SearchCount++;
                }
            }
            if (SearchCount == 0)
                throw new Exception($"There is not any string to search {searches.JoinToString(", ")}");
            LambdaExpression whereExpression = Expression.Lambda(body, parameter);
            MethodCallExpression resultExpression = WhereCall(source, whereExpression);
            return source.Provider.CreateQuery(resultExpression);
        }
        /// <summary>
        /// Sorting a queryable by property using expressions https://stackoverflow.com/questions/41244/dynamic-linq-orderby-on-ienumerablet-iqueryablet/233505#233505
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="source"></param>
        /// <param name="orderByProperty"></param>
        /// <param name="desc"></param>
        /// <returns></returns>
        public static IQueryable<T> OrderBy<T>(this IQueryable<T> source, string orderByProperty, bool desc)
        {
            string command = desc ? "OrderByDescending" : "OrderBy";
            Type type = typeof(T);
            PropertyInfo? property = type.GetProperty(orderByProperty) ?? throw new NotImplementedException($"Type {type.Name} does not contain property {orderByProperty}");
            ParameterExpression parameter = Expression.Parameter(type, "r");
            MemberExpression member = Expression.MakeMemberAccess(parameter, property);
            LambdaExpression lambda = Expression.Lambda(member, parameter);
            MethodCallExpression resultExpression = Expression.Call(typeof(Queryable), command,
                   [type, property.PropertyType], source.Expression, Expression.Quote(lambda));
            return source.Provider.CreateQuery<T>(resultExpression);
        }
        /// <summary>
        /// Find all values ​​of several fields based on the value of another field in a custom data table. All in strings
        /// </summary>
        /// <param name="Entity"></param>
        /// <param name="KeyField"></param>
        /// <param name="Field"></param>
        /// <param name="Value"></param>
        /// <param name="FirstOnly"></param>
        /// <returns></returns>
        public static async Task<List<Dictionary<string, object?>>> GetAllValues(this ApplicationDbContext db, Type Entity, bool FirstOnly, string SearchField, string SearchValue, params string[] Fields)
        {
            List<Dictionary<string, object?>> Values = [];
            IQueryable<object> source = (IQueryable<object>)db.Set(Entity).Where(SearchField, SearchValue, FilterType.Equals, false);
            if (FirstOnly) source = source.Take(1);
            List<object> Objects = await source.AsNoTracking().ToListAsync();
            foreach (object row in Objects)
            {
                Dictionary<string, object?> RowValues = [];
                foreach (string Field in Fields)
                {
                    if (!RowValues.ContainsKey(Field))
                        RowValues.Add(Field, row.GetPropertyValue(Field));
                }
                Values.Add(RowValues);
            }
            return Values;
        }
        /// <summary>
        /// Find all values ​​of several fields based on a list of values ​​of another field in a custom data table. All in strings
        /// </summary>
        /// <param name="Entity"></param>
        /// <param name="KeyField"></param>
        /// <param name="Field"></param>
        /// <param name="Value"></param>
        /// <param name="FirstOnly"></param>
        /// <returns></returns>
        public static async Task<List<Dictionary<string, object?>>> GetAllValues(this ApplicationDbContext db, Type Entity, string SearchField, List<string> SearchValues, params string[] Fields)
        {
            List<Dictionary<string, object?>> Values = [];
            int start = 0;
            List<string> SearchPage;
            do
            {
                SearchPage = [.. SearchValues.Skip(start).Take(Consts.MaxLinqContains)];
                if (SearchPage.Count == 0) break;
                IQueryable<object> source = (IQueryable<object>)db.Set(Entity).WhereContains(SearchField, SearchPage, Entity);
                List<object> Objects = await source.AsNoTracking().ToListAsync();
                foreach (object row in Objects)
                {
                    Dictionary<string, object?> RowValues = new()
                    {
                        { SearchField, row.GetPropertyValue(SearchField) }
                    };
                    foreach (string Field in Fields)
                    {
                        if (!RowValues.ContainsKey(Field))
                            RowValues.Add(Field, row.GetPropertyValue(Field));
                    }
                    Values.Add(RowValues);
                }
                start += Consts.MaxLinqContains;
            }
            while (SearchPage.Count > 0);
            return Values;
        }
        /// <summary>
        /// In the list of entries, set one to true, the rest to false
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="Source"></param>
        /// <param name="BoolProperty"></param>
        /// <param name="IdProperty"></param>
        /// <param name="IdValue"></param>
        /// <param name="db"></param>
        /// <param name="Set"></param>
        /// <returns></returns>
        public static async Task SetOneToTrue<T>(this IQueryable<T> Source, string BoolProperty, int IdValue, ApplicationDbContext db) where T : class
        {
            await Source.Where(BoolProperty, true).Where(BoolProperty, true).ExecuteUpdateAsync(typeof(T), BoolProperty, false);
            await db.ExecuteUpdateAsync(typeof(T), IdValue, BoolProperty, true);
        }
        /// <summary>
        /// Checking if a link exists in a list of tables and properties
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="db"></param>
        /// <param name="Id"></param>
        /// <param name="Fields"></param>
        /// <returns>Success if at least one link is found</returns>
        public static ActionResult IsAnyReferenceExists<T>(this ApplicationDbContext db, T? Id, params (Type TypeEntity, string Property)[] Fields)
        {
            foreach ((Type TypeEntity, string Property) in Fields)
            {
                if (db.Set(TypeEntity).Where(Property, Id, TypeEntity).Count() > 0)
                    return new ActionResult { IsSuccess = true, Message = "Reference is found in table {0}, property {1}", Params = [TypeEntity.Name, Property] };
            }
            return new ActionResult { IsSuccess = false };
        }
        /// <summary>
        /// Comparison of two sortings
        /// </summary>
        /// <param name="item1"></param>
        /// <param name="item2"></param>
        /// <returns></returns>
        public static bool SortCompare(Sort item1, Sort item2)
        {
            return item1.Name.Equals(item2.Name, StringComparison.OrdinalIgnoreCase) && item1.Direction.Equals(item2.Direction, StringComparison.OrdinalIgnoreCase);
        }
        /// <summary>
        /// Are there any related databases with a different topic, but not foreign ones?
        /// </summary>
        /// <param name="Config"></param>
        /// <returns></returns>
        public static bool AnyThemeNotForeignConnection(this IConfiguration Config)
        {
            return Config.GetSection("ConnectionStrings").GetChildren()
                .Any(r => !string.IsNullOrEmpty(r.Value) && r.Key.Contains("Theme", StringComparison.OrdinalIgnoreCase) && !r.Key.Contains("Foreign", StringComparison.OrdinalIgnoreCase));
        }
        /// <summary>
        /// Are there any related foreign databases with the same topic?
        /// </summary>
        /// <param name="Config"></param>
        /// <returns></returns>
        public static bool AnyForeignNotThemeConnection(this IConfiguration Config)
        {
            return Config.GetSection("ConnectionStrings").GetChildren()
                .Any(r => !string.IsNullOrEmpty(r.Value) && r.Key.Contains("Foreign", StringComparison.OrdinalIgnoreCase) && !r.Key.Contains("Theme", StringComparison.OrdinalIgnoreCase));
        }
        /// <summary>
        /// Extract the where clause from the query. Usage Source = Source.Where(WhereClause);
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="QuerySource"></param>
        /// <returns></returns>
        public static Expression<Func<T, bool>>? GetWhereClause<T>(this IQueryable<T> QuerySource)
        {
            if (QuerySource.Expression is not MethodCallExpression) return null;
            MethodCallExpression WhereExpression = (MethodCallExpression)QuerySource.Expression;
            if (WhereExpression.Method.Name != "Where") return null;
            UnaryExpression Arg1 = (UnaryExpression)WhereExpression.Arguments[1];
            return (Expression<Func<T, bool>>)Arg1.Operand;
        }
        /// <summary>
        /// Normally you need to search for square brackets as symbols, but if there is a ~ sign before the string, the square brackets are used for wildcard search in sql https://www.w3schools.com/sql/sql_wildcards.asp
        /// </summary>
        /// <param name=Consts.Search></param>
        /// <returns></returns>
        public static string SearchPattern(this string search) => search.StartsWith('~') ? search[1..] : search.Replace("[", "[[]");
        /// <summary>
        /// Generate an ordered list by a given property, filtering records with an empty value
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="entity"></param>
        /// <param name="propertyShowOrder"></param>
        /// <param name="NotNullsOnly">turn on filter</param>
        /// <param name="firstSortProp"></param>
        /// <returns></returns>
        public static async Task<List<T>> OrderedList<T>(this IQueryable<T> Source, string propertyShowOrder,
              bool NotNullsOnly = false, string? firstSortProp = null, CancellationToken Token = default) where T : class
        {
            List<T> orderedEntity = firstSortProp == null ? await Source.AsNoTracking().OrderBy(propertyShowOrder, false).ToListAsync(Token) :
                await ((IQueryable<T>)Source.AsNoTracking().OrderBy(firstSortProp, true).ThenBy(propertyShowOrder)).ToListAsync(Token);
            List<T> filteredEntity = NotNullsOnly ? [.. orderedEntity.Where(r => r.GetPropertyValue(propertyShowOrder) != null)] : orderedEntity;
            filteredEntity.Sort((x, y) =>
            {
                if (x.GetPropertyValue(propertyShowOrder) == null & y.GetPropertyValue(propertyShowOrder) == null) return 0;
                else if (x.GetPropertyValue(propertyShowOrder) == null & y.GetPropertyValue(propertyShowOrder) != null) return 1;
                else if (x.GetPropertyValue(propertyShowOrder) != null & y.GetPropertyValue(propertyShowOrder) == null) return -1;
                else if (firstSortProp != null && x.GetPropertyValue(firstSortProp) != null && y.GetPropertyValue(firstSortProp) == null) return 1;
                else if (firstSortProp != null && x.GetPropertyValue(firstSortProp) == null && y.GetPropertyValue(firstSortProp) != null) return -1;
                else if (firstSortProp != null && x.GetPropertyValue(firstSortProp) != null && y.GetPropertyValue(firstSortProp) != null &&
                    (int)x.GetPropertyValue(firstSortProp)! > (int)y.GetPropertyValue(firstSortProp)!) return 1;
                else if (firstSortProp != null && x.GetPropertyValue(firstSortProp) != null && y.GetPropertyValue(firstSortProp) != null &&
                    (int)x.GetPropertyValue(firstSortProp)! < (int)y.GetPropertyValue(firstSortProp)!) return -1;
                else if ((int)x.GetPropertyValue(propertyShowOrder)! > (int)y.GetPropertyValue(propertyShowOrder)!) return 1;
                else if ((int)x.GetPropertyValue(propertyShowOrder)! < (int)y.GetPropertyValue(propertyShowOrder)!) return -1;
                else return 0;
            });
            return filteredEntity;
        }
        /// <summary>
        /// Get a list of class properties that have a field in the database
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="entity"></param>
        /// <param name="source"></param>
        public static Collection<PropertyInfo> DbProperties<T>(this ApplicationDbContext db) where T : class, new()
        {
            Collection<PropertyInfo> DbProperties = [];
            T entity = new();
            PropertyInfo[] properties = typeof(T).GetProperties();
            foreach (PropertyInfo property in properties)
            {
                PropertyEntry? member = db.Entry(entity).Properties.FirstOrDefault(r => r.Metadata.Name == property.Name);
                if (member != null && member.Metadata.FindAnnotation("NotMapped") == null &&
                    !db.Entry(entity).Navigations.Any(r => r.Metadata.Name == property.Name))
                    DbProperties.Add(property);
            }
            return DbProperties;
        }
        /// <summary>
        /// Re-sorting a custom table by a custom field. An element with a given key is shown above others with the same order. Secondary sorting by key
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="db"></param>
        /// <param name="orderField"></param>
        /// <param name="keyField"></param>
        /// <param name="keyValue"></param>
        /// <returns></returns>
        public static async Task ReSort<TItem, TValue>(this ApplicationDbContext db, string orderField, string keyField, TValue? keyValue) where TItem : class
        {
            List<TItem> oldList = await db.Set<TItem>().ToListAsync();
            oldList.Sort((x, y) =>
            {
                int? xOrderValue = (int?)x.GetPropertyValue(orderField);
                int? yOrderValue = (int?)y.GetPropertyValue(orderField);
                TValue? xKeyValue = (TValue?)x.GetPropertyValue(keyField);
                TValue? yKeyValue = (TValue?)y.GetPropertyValue(keyField);
                if (xOrderValue == null & yOrderValue == null) return 0;
                else if (xOrderValue == null) return 1;
                else if (yOrderValue == null) return -1;
                else if ((int)xOrderValue > (int)yOrderValue) return 1;
                else if ((int)xOrderValue < (int)yOrderValue) return -1;
                else if (xKeyValue?.ToString() == keyValue?.ToString()) return -1;
                else if (yKeyValue?.ToString() == keyValue?.ToString()) return 1;
                else return string.Compare(xKeyValue?.ToString(), yKeyValue?.ToString(), true, CultureInfo.InvariantCulture);
            });
            for (int i = 0; i < oldList.Count; i++)
            {
                PropertyInfo? OrderProperty = oldList[i].GetType().GetProperty(orderField);
                if (OrderProperty?.GetValue(oldList[i]) != null)
                    OrderProperty.SetValue(oldList[i], i);
            }
            await db.SaveChangesAsync();
            db.DetachAllEntries(typeof(TItem));
        }
        /// <summary>
        /// Selecting a page as a result of a query, selecting only unique elements
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="source"></param>
        /// <param name="Skip"></param>
        /// <param name="Take"></param>
        /// <returns></returns>
        public static IEnumerable<int>? DistinctSkipAndTake(this IEnumerable<int> source, int Skip, int Take)
        {
            if (source == null || !source.Any()) return source;
            List<int> Skipped = [];
            List<int> Taken = [];
            foreach (int item in source)
            {
                if (!Skipped.Contains(item) && !Taken.Contains(item))
                {
                    if (Skipped.Count < Skip)
                        Skipped.Add(item);
                    else
                    {
                        Taken.Add(item);
                        if (Taken.Count >= Take)
                            return Taken;
                    }
                }
            }
            return Taken;
        }
        /// <summary>
        /// Selecting a sorting method name
        /// </summary>
        /// <param name="SortDirection"></param>
        /// <param name="first"></param>
        /// <returns></returns>
        public static string GetSortMethodName(string SortDirection, bool first = true)
        {
            return first ? (SortDirection.Equals(nameof(ListSortDirection.Descending)
                .ToString(CultureInfo.InvariantCulture), StringComparison.OrdinalIgnoreCase) ? "OrderByDescending" : "OrderBy")
               : (SortDirection.Equals(nameof(ListSortDirection.Descending)
                .ToString(CultureInfo.InvariantCulture), StringComparison.OrdinalIgnoreCase) ? "ThenByDescending" : "ThenBy");
        }
        /// <summary>
        /// Extension for selecting a specific property
        /// </summary>
        /// <typeparam name="TEntity"></typeparam>
        /// <param name="source"></param>
        /// <param name="Property"></param>
        /// <returns></returns>
        public static IQueryable SelectProperty(this IQueryable queryable, string Property, Type type)
        {
            PropertyInfo? property = type.GetProperty(Property) ?? throw new Exception($"Type {nameof(type)} has not property {Property}");
            ParameterExpression parameter = Expression.Parameter(type, "r");
            MemberExpression member = Expression.MakeMemberAccess(parameter, property);
            LambdaExpression lambda = Expression.Lambda(member, parameter);
            MethodInfo MethodSelect = typeof(Queryable).GetMethods(BindingFlags.Static | BindingFlags.Public)
                .First(m => m.Name.Equals(nameof(Queryable.Select), StringComparison.OrdinalIgnoreCase) && m.GetParameters().Length == 2).MakeGenericMethod(type, property.PropertyType);
            MethodInfo MethodCast = typeof(Queryable).GetMethods(BindingFlags.Static | BindingFlags.Public)
                .First(m => m.Name.Equals(nameof(Queryable.Cast), StringComparison.OrdinalIgnoreCase)).MakeGenericMethod(type);
            MethodCallExpression resultExpression = Expression.Call(null, MethodSelect, Expression.Call(null, MethodCast, queryable.Expression), Expression.Quote(lambda));  // Expression.Convert(queryable.Expression, type.QueryableType())
            return queryable.Provider.CreateQuery(resultExpression);
        }
        /// <summary>
        /// Alert if the number of records in one of the database tables has decreased by more than the specified percentage, default 3
        /// </summary>
        /// <param name="db"></param>
        /// <param name="CriticalDropPercentage"></param>
        /// <returns></returns>
        public static async Task<string?> CountReport(this ApplicationDbContext Db, int DaySpan = 1)
        {
            List<PropertyInfo> DbProperties = Db.GetDbSetProperties();
            Dictionary<string, int> TableCounts = [];
            string? PreviousData = await Db.GetAppData(nameof(TableCounts));
            JsonNode? Previous = PreviousData == null ? null : JsonNode.Parse(PreviousData);
            DateTime? PreviousDateTime = Previous == null ? null : JsonSerializer.Deserialize<DateTime?>(Previous?["Time"]);
            Dictionary<string, int> PreviousCounts = Previous == null ? [] : JsonSerializer.Deserialize<Dictionary<string, int>?>(Previous?["Counts"]) ?? [];
            string Message = "<table><tr><th>[Table]</th><th>[Previous Count]</th><th>[Count]</th><th>[Difference]</th><th>[Difference], %</th></tr>";
            if (PreviousDateTime != null && ((DateTime)PreviousDateTime).AddDays(DaySpan) > DateTime.Now) return null;
            foreach (PropertyInfo Prop in DbProperties)
            {
                string Name = Prop.Name;
                var a = Db.Model.FindEntityType(Name);
                var b = Db.Model.ContainsProperty(Name);
                IQueryable<object>? dbSet = (IQueryable<object>?)Prop.GetValue(Db, null);
                if (dbSet != null)
                {
                    int Count = await dbSet.CountAsync();
                    if (!string.IsNullOrEmpty(Name) && !TableCounts.ContainsKey(Name) && Count > 0)
                    {
                        TableCounts.Add(Name, Count);
                        int PreviousCount = PreviousCounts.TryGetValue(Name, out int count) ? count : 0;
                        string Style = Count < PreviousCount ? "color:red" : (Count > PreviousCount ? "color:green" : string.Empty);
                        decimal ChangePercent = PreviousCount == 0 ? 0 : ((Count - PreviousCount) / PreviousCount);
                        Message += $"<tr><td>{Name}</td><td>{PreviousCount}</td><td>{Count}</td><td style=\"{Style}\">{Count - PreviousCount}</td><td style=\"{Style}\">{(ChangePercent == 0 ? string.Empty : ChangePercent.ToString("p1"))}</td></tr>";
                    }
                }
            }
            Message += "</table>";
            await Db.SetJsonData(nameof(TableCounts), new { Time = DateTime.Now, Counts = TableCounts });
            return Message;
        }
    }
}
