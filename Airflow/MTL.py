# COMBINE CSV AND DB DATA
def merge(data_csv, data_api):
    csv = json.loads(data_csv)
    csv_df = pd.json_normalize(csv)
    api = json.loads(data_api)
    api_df = pd.json_normalize(api)
    logging.info("Data merging process started...")

    merged_data = pd.merge(csv_df, api_df, left_on='artist', right_on='artists', how='inner')
    merged_data['decade'] = merged_data['year'].apply(decade)
    if 'grammy_id' in merged_data.columns:
        merged_data.drop_duplicates(subset='grammy_id', inplace=True)
    col_interest = ['year', 'category', 'nominee', 'artist', 'was_nominated', 
                           'track_id', 'artists', 'track_name', 'popularity', 'danceability', 
                           'energy', 'valence', 'album_name', 'explicit', 'decade']
    
    merged_data = merged_data[col_interest]
    merged_df = api_df.merge(csv_df, how="inner", left_on='track_name', right_on='nominee')
    merged_df['nomination_dec'] = merged_df['year'].apply(lambda x: (x // 10) * 10)
    merged_df.drop(['artists', 'nominee'], axis=1, inplace=True)
    col_interest = ['year', 'category', 'was_nominated', 'artist',
                    'track_name', 'popularity', 'danceability', 
                    'energy', 'explicit', 'nomination_dec']
   
    merged_data = merged_df[col_interest]
    logging.info("Data merging process successfully completed.")