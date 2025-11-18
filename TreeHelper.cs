using BigLog.Data;
using Microsoft.EntityFrameworkCore;

namespace BigLog.Utilities
{
    public static class TreeHelper
    {
        /// <summary>
        /// Set the value of a property on all parent list items
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="Items"></param>
        /// <param name="Id"></param>
        /// <param name="ParentId"></param>
        /// <param name="KeyValue"></param>
        /// <param name="Property"></param>
        /// <param name="Value"></param>
        public static void SetToAllParent<T>(this List<T> Items, string Id, string ParentId, T? item, string Property, object Value)
        {
            if (item == null) return;
            item.SetPropertyValue(Property, Value);
            if (item.GetPropertyValue(ParentId) == null) return;
            SetToAllParent(Items, Id, ParentId,
                Items.Where(r => r.GetPropertyValue(Id)?.ToString() == item.GetPropertyValue(ParentId)?.ToString()).FirstOrDefault(), Property, Value);
        }
        /// <summary>
        /// Add all parents of list items to the list
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="Childs"></param>
        /// <param name="Source"></param>
        /// <param name="Id"></param>
        /// <param name="ParentId"></param>
        public static void AddAllParents<T>(this List<T> Children, List<T> Items, string Id, string ParentId)
        {
            if (Children == null) return;
            for (int i = 0; i < Children.Count; i++)
            {
                if (Children[i].GetPropertyValue(ParentId) != null)
                {
                    T? Parent = Items.Where(r => r.GetPropertyValue(Id)?.ToString() == Children[i].GetPropertyValue(ParentId)?.ToString()).FirstOrDefault();
                    if (Parent != null)
                    {
                        string? ParentIdValue = Parent.GetPropertyValue(Id)?.ToString();
                        if (!Children.Any(r => r.GetPropertyValue(Id)?.ToString() == ParentIdValue))
                            Children.Add(Parent);
                    }
                }
            }
        }
        /// <summary>
        /// Select all leaves of a single root
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="Items">Source</param>
        /// <param name="IdProperty"></param>
        /// <param name="ParentIdProperty"></param>
        /// <param name="item">Root</param>
        /// <param name="To">Destination collection</param>
        /// <param name="LeavesOnly">Only leaves</param>
        /// <param name="LevelCounter">Only a few first levels</param>
        /// <param name="StopCondition">Stop condition</param>
        public static void SelectAllLeaves<T>(this IEnumerable<T> Items, string IdProperty, string ParentIdProperty, T? item,
            ref List<T> To, bool LeavesOnly = false, int LevelCounter = int.MaxValue)
        {
            if (!Items.Any()) return;
            List<T> Children = [.. Items.Where(r => r.GetPropertyValue(ParentIdProperty)?.ToString() == item?.GetPropertyValue(IdProperty)?.ToString())];
            LevelCounter--;
            if (LevelCounter > 0)
                foreach (T Child in Children)
                {
                    Items.SelectAllLeaves(IdProperty, ParentIdProperty, Child, ref To, LeavesOnly, LevelCounter);
                }
            if (item != null && LeavesOnly && To.Contains(item))
                To.RemoveAll(r => r.GetPropertyValue(IdProperty)?.ToString() == item.GetPropertyValue(IdProperty)?.ToString());
            To.AddRange(Children);
        }
        /// <summary>
        /// Select all parents of an element up the branch into a list
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="Items"></param>
        /// <param name="Id"></param>
        /// <param name="ParentId"></param>
        /// <param name="KeyValue"></param>
        /// <param name="Property"></param>
        /// <param name="Value"></param>
        public static void SelectAllParent<T>(this List<T> Items, string Id, string ParentId, T? item, ref List<T> To)
        {
            To ??= [];
            if (item?.GetPropertyValue(ParentId) == null) return;
            T? Parent = Items.Where(r => r.GetPropertyValue(Id)?.ToString() == item.GetPropertyValue(ParentId)?.ToString()).FirstOrDefault();
            if (Parent == null) return;
            To = [.. To.Prepend(Parent)];
            SelectAllParent(Items, Id, ParentId, Parent, ref To);
        }
        /// <summary>
        /// For all tree branches, starting from item, the Property property is set to Value, unless this property already has another value. Otherwise, the last value is inherited.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="Items"></param>
        /// <param name="Id"></param>
        /// <param name="ParentId"></param>
        /// <param name="item"></param>
        /// <param name="Property"></param>
        /// <param name="Value"></param>
        public static void InheritPropertyValue<T>(this List<T> Items, string Id, string ParentId, T? item, string Property, object? Value)
        {
            if (item?.GetPropertyValue(Property) != null)
                Value = item.GetPropertyValue(Property);
            else if (item != null && Value != null)
                item.SetPropertyValue(Property, Value);
            Items.Where(r => r.GetPropertyValue(ParentId)?.ToString() == item?.GetPropertyValue(Id)?.ToString()).ToList()
                .ForEach(r => Items.InheritPropertyValue(Id, ParentId, r, Property, Value));
        }
        /// <summary>
        /// Select the parent element that first has a non-null value
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="Items"></param>
        /// <param name="Id"></param>
        /// <param name="ParentId"></param>
        /// <param name="KeyValue"></param>
        /// <param name="Property"></param>
        /// <param name="Value"></param>
        public static async Task<T?> SelectNotNullParent<T>(this IQueryable<T> queryable, T? item, string IdField, string ParentIdField, string CheckField) where T : class
        {
            if (item == null) return null;
            object? value = item.GetPropertyValue(CheckField);
            if (value != null) return item;
            if (item.GetPropertyValue(ParentIdField) == null) return null;
            T? Parent = await queryable.Where(IdField, item.GetPropertyValue(ParentIdField)).FirstOrDefaultAsync();
            if (Parent == null) return null;
            return await SelectNotNullParent(queryable, Parent, IdField, ParentIdField, CheckField);
        }
        /// <summary>
        /// Select the parent element that first has a non-null value
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="Items"></param>
        /// <param name="Id"></param>
        /// <param name="ParentId"></param>
        /// <param name="KeyValue"></param>
        /// <param name="Property"></param>
        /// <param name="Value"></param>
        public static T? SelectNotNullParent<T>(this List<T> source, T item, string IdField, string ParentIdField, string CheckField) where T : class
        {
            if (item == null) return null;
            object? value = item.GetPropertyValue(CheckField);
            if (value != null) return item;
            if (item.GetPropertyValue(ParentIdField) == null) return null;
            T? Parent = source.FirstOrDefault(r => r.GetPropertyValue(IdField)?.ToString() == item.GetPropertyValue(ParentIdField)?.ToString());
            if (Parent == null) return null;
            return SelectNotNullParent(source, Parent, IdField, ParentIdField, CheckField);
        }
        /// <summary>
        /// Select the root element
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="Items"></param>
        /// <param name="Id"></param>
        /// <param name="ParentId"></param>
        /// <param name="KeyValue"></param>
        /// <param name="Property"></param>
        /// <param name="Value"></param>
        public static async Task<T> SelectRoot<T>(this IQueryable<T> queryable, T item, string IdField, string ParentIdField) where T : class
        {
            if (item.GetPropertyValue(ParentIdField) == null) return item;
            T? Parent = await queryable.Where(IdField, item.GetPropertyValue(ParentIdField)).FirstOrDefaultAsync();
            if (Parent == null) return item;
            return await SelectRoot(queryable, Parent, IdField, ParentIdField);
        }
        /// <summary>
        /// Select root elements with minimal requests. Synchronous use only.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="queryable"></param>
        /// <param name="Items"></param>
        /// <param name="IdField"></param>
        /// <param name="ParentIdField"></param>
        /// <returns></returns>
        public static async Task SelectRoots<T>(this IQueryable<T> queryable, List<T> Items, string IdField, string ParentIdField) where T : class
        {
            if (Items == null) return;
            List<T> ChildItems = [.. Items.Where(r => r.GetPropertyValue(ParentIdField) != null)];
            if (ChildItems.Count == 0) return;
            List<int> ChildIds = [.. ChildItems.Where(r => r.GetPropertyValue(IdField) != null).Select(r => (int)r.GetPropertyValue(IdField)!)];
            List<int> ParentIds = [.. ChildItems.Select(r => (int)r.GetPropertyValue(ParentIdField)!)];
            List<T> ParentItems = await queryable.WhereContains(IdField, ParentIds).AsNoTracking().ToListAsync();
            if (ParentItems.Count == 0) return;
            Items.RemoveAll(r => r.GetPropertyValue(IdField) != null && ChildIds.Contains((int)r.GetPropertyValue(IdField)!));
            Items.AddRange(ParentItems);
            await SelectRoots(queryable, Items, IdField, ParentIdField);
        }
        /// <summary>
        /// Select the chain of parent elements from the given child element, extending itself until it reaches null
        /// </summary>
        public static async Task<List<T>> SelectAllParent<T, TValue>(this IQueryable<T> DataSource, List<T> Items, string IdProperty, string ParentIdProperty,
            int? MaxLength = null, CancellationToken token = default) where T : class
        {
            if (Items.Count == 0 || MaxLength == 0) return Items;
            object? ParentIdValue = Items[0].GetPropertyValue(ParentIdProperty);
            if (ParentIdValue == null) return Items;
            T? Parent = await ((IQueryable<T>)DataSource.AsNoTracking().Where((TValue)ParentIdValue, typeof(T), typeof(TValue), IdProperty))
                .FirstOrDefaultAsync(token);
            if (Parent == null) return Items;
            Items = [.. Items.Prepend(Parent)];
            return await DataSource.SelectAllParent<T, TValue>(Items, IdProperty, ParentIdProperty,
                MaxLength == null ? null : (int)MaxLength - 1, token);
        }
        /// <summary>
        /// Select an unordered list of child elements from a given list of parent elements.
        /// For simplicity, the key is always an int, and the parent reference is always an int?
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="TValue"></typeparam>
        /// <param name="Items">List of parent elements with at least one member</param>
        /// <param name="IdProperty"></param>
        /// <param name="ParentIdProperty"></param>
        /// <param name="DataSource"></param>
        /// <param name="IncludePath">Include or exclude elements of a branch, leaving only leaves</param>
        /// <returns></returns>
        public static async Task SelectAllLeaves<T>(this List<T>? Items, string IdProperty, string ParentIdProperty,
            IQueryable<T> DataSource, bool LeavesOnly = false, CancellationToken token = default) where T : class
        {
            if (Items == null || Items.Count == 0) return;
            List<int> ParentIdValues = [.. Items.Select(r => (int?)r.GetPropertyValue(IdProperty)).OfType<int>()];
            List<T> Childs = await DataSource.AsNoTracking().WhereContains(ParentIdProperty, ParentIdValues).ToListAsync(token);
            await Childs.SelectAllLeaves(IdProperty, ParentIdProperty, DataSource, LeavesOnly, token);
            if (LeavesOnly)
                Items.RemoveAll(r => Childs.Any(r => (int?)r.GetPropertyValue(ParentIdProperty) == (int?)r.GetPropertyValue(IdProperty)));
            Items.AddRange(Childs);
        }
        /// <summary>
        /// Re-sorting an arbitrary tree table by an arbitrary field. Secondary sorting by key
        /// </summary>
        /// <typeparam name="TItem"></typeparam>
        /// <typeparam name="TValue"></typeparam>
        /// <param name="db"></param>
        /// <param name="orderField"></param>
        /// <param name="keyField"></param>
        /// <param name="keyValue">An element with a given key is shown above others with the same order.</param>
        /// <returns></returns>
        public static async Task ReSortTree<TItem>(ApplicationDbContext db, string orderField, string keyField, string parentField, int? keyValue) where TItem : class
        {
            List<TItem> oldList = await db.Set<TItem>().ToListAsync();
            ReSortTreeLevel(orderField, keyField, parentField, keyValue, oldList, null, 0);
            await db.SaveChangesAsync();
            db.DetachAllEntries(typeof(TItem));
        }
        /// <summary>
        /// A recursive function that sorts elements at the same level in a branch and calls itself to sort nested levels.
        /// </summary>
        /// <typeparam name="TItem"></typeparam>
        /// <param name="orderField"></param>
        /// <param name="keyField"></param>
        /// <param name="keyValue"></param>
        /// <param name="oldList"></param>
        /// <param name="RootId"></param>
        /// <param name="iCounter"></param>
        /// <returns></returns>
        public static int ReSortTreeLevel<TItem>(string orderField, string keyField, string parentField, int? keyValue, List<TItem> oldList, int? RootId, int iCounter) where TItem : class
        {
            List<TItem> Level = [.. oldList.Where(r => (int?)r.GetPropertyValue(parentField) == RootId)];
            Level.Sort((x, y) =>
            {
                int? xOrderValue = (int?)x.GetPropertyValue(orderField);
                int? yOrderValue = (int?)y.GetPropertyValue(orderField);
                int? xKeyValue = (int?)x?.GetPropertyValue(keyField);
                int? yKeyValue = (int?)y?.GetPropertyValue(keyField);
                if (xOrderValue == null & yOrderValue == null) return 0;
                else if (xOrderValue == null) return 1;
                else if (yOrderValue == null) return -1;
                else if ((int)xOrderValue > (int)yOrderValue) return 1;
                else if ((int)xOrderValue < (int)yOrderValue) return -1;
                else if (xKeyValue == yKeyValue) return 0;
                else if (xKeyValue == keyValue) return -1;
                else if (yKeyValue == keyValue) return 1;
                else if (xOrderValue == null) return 1;
                else if (yOrderValue == null) return -1;
                else return ((int)xKeyValue!).CompareTo((int)yKeyValue!);
            });
            foreach (TItem item in Level)
            {
                item.SetPropertyValue(orderField, iCounter);
                iCounter++;
                iCounter = ReSortTreeLevel(orderField, keyField, parentField, keyValue, oldList, (int?)item.GetPropertyValue(keyField), iCounter);
            }
            return iCounter;
        }
    }
}
