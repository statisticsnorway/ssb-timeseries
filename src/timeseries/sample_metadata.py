from timeseries import configuration
import json
#import dapla as dp

class Meta:
    def __init__(self, str: dataset_name, str: dataset_type, dict: dataset_tags=None, dict: series_tags=None, str:metadata_file=None):
        """
        Meta class
        """

        self.dataset_name = dataset_name
        self.dataset_type = dataset_type
        self.dataset_tags = dataset_tags
        self.series_tags = series_tags
        self.metadata_file = metadata_file
        self.load_metadata()

    def tag(self, attr, value):
        self.dataset_tags[attr] = value

    def get_tag(self, series_name):
        return self.metadata.get(column_name, "No metadata found")

    def list_columns(self):
        return self.data.columns

"""def create_meta(separator: str = '_', **kwargs):"""
def create_meta(dataset: str = '<new sample dataset>', series, **tags) -> Meta:
    """
    Generate sample data with specified date range and lists.

    Parameters:
    - lists.
    Returns:
    - Object.

    Example:
    ```
    # Generate sample data with no specified start or end date (defaults to +/- infinity)
    sample_data = generate_sample_df(List1, List2, freq='D')
    ```
    """

    meta = Meta{dataset}

    
    return meta

""" 
    def load_metadata(self):
        with open(self.metadata_file, 'r') as file:
            self.metadata = json.load(file)

    def save_metadata(self):
        with open(self.metadata_file, 'w') as file:
            json.dump(self, file, indent=4) 
    """