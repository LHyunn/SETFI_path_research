import pandas as pd
import numpy as np

class SETFI_Class:
    def __init__(self, dataframe):
        self.dataframe = pd.read_csv(dataframe)
        self.dataframe_locn = self.dataframe.filter(regex='locn')
        self.dataframe_res = self.dataframe.filter(regex='res')
        self.dataframe = pd.concat([self.dataframe_locn, self.dataframe_res], axis=1)
        self.dataframe = self.dataframe.replace(" ", np.nan)
        self.dataframe = self.dataframe.sort_values(by=['res'], ascending=False)
        
    def get_row(self, row_index):
        return self.dataframe.iloc[row_index]
        
    def get_NaN_index(self, row_index):
        row = self.get_row(row_index)
        index_list = []
        for i in range(len(row.isnull().values)):
            if row.isnull().values[i] == True:
                index_list.append(i)
        return index_list
    
    def get_window_dataframe(self, window_size, index, verbose=False):
        start_index = max(0, (index - window_size // 2))
        end_index = min(len(self.dataframe), (index + window_size // 2))
        print('start_index: ', start_index, 'end_index: ', end_index) if verbose else None
        return self.dataframe.iloc[start_index:end_index]
    
    def fill_NaN(self, window_size, row_index, verbose=False):
        #row에 NaN이 있는 index를 가져옴
        NaN_index = self.get_NaN_index(row_index)
        print('NaN_index: ', NaN_index, "Total NaN: ", len(NaN_index)) if verbose else None
        #해당 인덱스를 돌면서 NaN을 채움. extract_df의 해당 인덱스에서 많이 등장하는 값을 가져옴.
        extracted_df = self.get_window_dataframe(window_size, row_index, verbose)
        for i in NaN_index:
            value_counts = extracted_df.iloc[:,i].value_counts()
            #rank만큼 자름
            value_counts = value_counts[:1]
            #print('value_counts: ', len(value_counts)) if verbose else None
            rand_index = np.random.randint(0, len(value_counts))
            #NaN을 채움
            self.dataframe.iloc[row_index, i] = value_counts.index[rand_index]
            print('replace: ', value_counts.index[rand_index], '\tat column : ', i, 'row : ', row_index, 'because', value_counts.index[rand_index], 'is most frequent, count: ', value_counts.values[rand_index]) if verbose else None
        
                
def main():
    data = SETFI_Class('/path/to/SETFI.csv')
    for i in range(len(data.dataframe)):
        data.fill_NaN(100, i, verbose=True)
    data.dataframe.to_csv(f'SETFI_imputed.csv', index=False)
        

if __name__ == "__main__":
    main()