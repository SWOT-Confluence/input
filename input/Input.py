class Input:
    """
    A class that represents Input operations.
    
    Attributes
    -----------
    extract_strategy: ExtractStrategy
        extract strategy object used to extract SWOT observations
    write_strategy: WriteStrategy
        write strategy object used to write NetCDF time series
    
    Methods
    -------
    execute_strategies()
        run input operations: extract observations and write NetCDF file.
    get_exe_data(input_json)
        retrun dictionary of data required to execution input operations.
    """
    
    def __init__(self, extract_strategy, write_strategy):
        """
        Parameters
        ----------
        extract_strategy: ExtractStrategy
            extract strategy object used to extract SWOT observations
        write_strategy: WriteStrategy
            write strategy object used to write NetCDF time series
        """
        
        self.extract_strategy = extract_strategy
        self.write_strategy = write_strategy
    
    def set_strategies(self, extract_strategy, write_strategy):
        """Set strategies used to execute input extract and write operations.
        
        Parameters
        ----------
        extract_strategy: ExtractStrategy
            extract strategy object used to extract SWOT observations
        write_strategy: WriteStrategy
            write strategy object used to write NetCDF time series
        """
        
        self.extract_strategy = extract_strategy
        self.write_strategy = write_strategy
    
    def execute_strategies(self):
        """Run input operations: extract observations and write NetCDF file."""
                
        # Extract data
        self.extract_strategy.extract()
                        
        # Write data
        self.write_strategy.write(self.extract_strategy.data, self.extract_strategy.obs_times)
        
    def execute_strategies_local(self):
        """Run input operations: extract observations and write NetCDF file."""
                
        # Extract data
        self.extract_strategy.extract_local()
                        
        # Write data
        self.write_strategy.write(self.extract_strategy.data, self.extract_strategy.obs_times)