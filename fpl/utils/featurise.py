def add_prev_groupby(df_init, col_group, col_sort, col_shift):
    indexes = col_group + [col_sort]
    s_next = df_init.groupby(col_group).apply(lambda x: resample_shift_prev(x, col_index=col_sort, col_shift=col_shift))
    df_next = s_next.reset_index(level=indexes)

    # Set columns to merge as objects, otherwise pandas  might change column dtype then complain it can't merge
    df_init[indexes] = df_init[indexes].apply(lambda x: x.astype('object'))
    df_next[indexes] = df_next[indexes].apply(lambda x: x.astype('object'))

    df_final = df_init.merge(df_next, how='left', on=indexes, suffixes=('', '_prev'))
    return df_final


def resample_shift_prev(df, col_index, col_shift):
    df_index = df.set_index(col_index)
    new_index = range(min(df_index.index), max(df_index.index) + 1)
    df_resampled = df_index.reindex(new_index)
    df_shift = df_resampled.shift(1)[col_shift]
    return df_shift
