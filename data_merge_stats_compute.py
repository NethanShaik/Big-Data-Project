import pandas as pd
import os
import time
from tqdm import tqdm

# Specify the path to the Parquet file
file_path = "/Users/nethanshaik/Desktop/Big_Data_Project/dataset/netflix_processed_data/netflix_data.parquet"

# Read the Parquet file
df = pd.read_parquet(file_path)


# Display the first few rows
print(df.head())

# Show number of rows, columns, and column names
print(f"Number of rows: {df.shape[0]}")
print(f"Number of columns: {df.shape[1]}")
print("Columns:", df.columns.tolist())


class NetflixDataMerger:
    def __init__(self):
        """Initialize the merger with timing functionality"""
        self.start_time = time.time()
        
    def log(self, message):
        """Log messages with timestamp"""
        elapsed = time.time() - self.start_time
        print(f"[{elapsed:.2f}s] {message}")
        
    def process_ratings_file(self, ratings_file):
        """Process a single ratings file and return as DataFrame"""
        self.log(f"Processing ratings file: {os.path.basename(ratings_file)}")
        
        ratings_data = []
        current_movie_id = None
        chunk_size = 100000
        current_chunk = []
        
        # Read and process the file line by line
        with open(ratings_file, 'r') as file:
            for line in tqdm(file, desc="Reading ratings"):
                line = line.strip()
                
                if line.endswith(':'):
                    # This is a movie ID line
                    current_movie_id = int(line[:-1])
                else:
                    try:
                        # This is a rating line
                        user_id, rating, date = line.split(',')
                        current_chunk.append({
                            'MovieID': current_movie_id,
                            'CustomerID': int(user_id),
                            'Rating': int(rating),
                            'Date': pd.to_datetime(date)
                        })
                        
                        if len(current_chunk) >= chunk_size:
                            ratings_data.extend(current_chunk)
                            current_chunk = []
                            
                    except Exception as e:
                        continue
        
        # Add remaining chunk
        if current_chunk:
            ratings_data.extend(current_chunk)
            
        # Convert to DataFrame
        df = pd.DataFrame(ratings_data)
        self.log(f"Processed {len(df):,} ratings from file")
        return df
    
    def merge_datasets(self, ratings_files, movies_file, output_path):
        """Merge multiple ratings files with movie data"""
        try:
            # Read movies data
            self.log("Reading movies data...")
            movies_df = pd.read_csv(movies_file, 
                                  names=['MovieID', 'Year', 'Title'],
                                  encoding='latin-1')
            self.log(f"Loaded {len(movies_df):,} movies")
            
            # Process each ratings file and concatenate
            all_ratings = []
            for ratings_file in ratings_files:
                ratings_df = self.process_ratings_file(ratings_file)
                all_ratings.append(ratings_df)
            
            # Concatenate all ratings
            self.log("Concatenating all ratings...")
            combined_ratings = pd.concat(all_ratings, ignore_index=True)
            
            # Merge with movies data
            self.log("Merging with movie data...")
            merged_df = pd.merge(combined_ratings, movies_df, on='MovieID', how='left')
            
            # Create output directory
            os.makedirs(output_path, exist_ok=True)
            
            # Save the merged dataset
            self.log("Saving merged dataset...")
            merged_df.to_parquet(
                f"{output_path}/netflix_merged_data.parquet",
                engine='pyarrow',
                compression='snappy'
            )
            
            # Create and save summary
            self.log("Creating summary statistics...")
            summary_df = merged_df.groupby(['MovieID', 'Title', 'Year']).agg({
                'Rating': ['count', 'mean'],
                'CustomerID': 'nunique'
            }).reset_index()
            
            summary_df.columns = [
                'MovieID', 'Title', 'Year', 
                'total_ratings', 'avg_rating', 'unique_users'
            ]
            
            summary_df.to_csv(f"{output_path}/netflix_merged_summary.csv", index=False)
            
            # Print final statistics
            self.log("\nFinal Statistics:")
            self.log(f"Total Movies: {len(merged_df['MovieID'].unique()):,}")
            self.log(f"Total Users: {len(merged_df['CustomerID'].unique()):,}")
            self.log(f"Total Ratings: {len(merged_df):,}")
            self.log(f"Date Range: {merged_df['Date'].min()} to {merged_df['Date'].max()}")
            
            return merged_df
            
        except Exception as e:
            self.log(f"Error occurred: {str(e)}")
            raise

def main():
    # File paths - update these to match your file locations
    ratings_files = [
        '/Users/nethanshaik/Desktop/Big_Data_Project/dataset/combined_data_1.txt',
        '/Users/nethanshaik/Desktop/Big_Data_Project/dataset/combined_data_2.txt',
        '/Users/nethanshaik/Desktop/Big_Data_Project/dataset/combined_data_3.txt',
        '/Users/nethanshaik/Desktop/Big_Data_Project/dataset/combined_data_4.txt'
    ]
    movies_file = '/Users/nethanshaik/Desktop/Big_Data_Project/dataset/movie_titles_edited.csv'
    output_path = '/Users/nethanshaik/Desktop/Big_Data_Project/dataset/netflix_merged_output'
    
    # Initialize and run merger
    merger = NetflixDataMerger()
    merged_data = merger.merge_datasets(
        ratings_files=ratings_files,
        movies_file=movies_file,
        output_path=output_path
    )

if __name__ == "__main__":
    main()
