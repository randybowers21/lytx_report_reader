from datetime import datetime, timedelta
import os

import pandas as pd

def get_month_start(date: datetime=datetime.today()):
    month_start = (date - timedelta(days=date.day)).replace(day=1).date()
    return month_start

def get_month_end(date: datetime=datetime.today()):
    month_end = (date - timedelta(days=date.day)).date()
    return month_end


class Report:
    def __init__(self, report_directory:str, start_date: datetime=get_month_start(), end_date: datetime=get_month_end()):
        self.report_directory = report_directory
        self.start_date = start_date
        self.end_date = end_date

    def merge_reports(self, left_df: pd.DataFrame, right_df: pd.DataFrame, merge_column:str) -> pd.DataFrame:
        print('INFO: Merging reports')
        dataframe =  pd.merge(left_df, right_df, how='left', on=merge_column)
        dataframe = dataframe.fillna(0)
        return dataframe

class EventsReport(Report):
    def __init__(self, report_directory, start_date: datetime=get_month_start(), end_date: datetime=get_month_end()):
        super().__init__(report_directory, start_date, end_date) 
        print('EVENT INFO: Reading Individual Reports')
        try:
            self.read_all_reports()
        except TypeError as e:
            print(e)
        print('EVENT INFO: Combining Reports')
        self.base_report = self.combine_all_dataframes()
        print('EVENT INFO: Creating Final Events Report')
        self.final_report = self.create_final_report()


    def read_csv_to_dataframe(self, file_name: str, report_type: str) -> pd.DataFrame:
        """Returns dataframe from CSV

        Args:
            file_name (str): Location of the file
            report_type (str): Name of the camera events that are being tracked in CSV. Lytx Reports doesnt name CSV appropriately.

        Returns:
            pd.DataFrame: Basic Dataframe with updated column names
        """
        dataframe = pd.read_csv(file_name)
        dataframe = dataframe.drop(columns=['Total Score_Total', 'Total Score_Trend', 'Total Events_Trend', 'Recent Notes'])
        dataframe = dataframe.rename(columns={'Total Events_Total': report_type})
        dataframe = dataframe.fillna(0)
        return dataframe

    def read_all_reports(self) -> None:
        """
            Loops through files in directory and reads each CSV in folder and adds to list of dataframes.
        Raises:
            TypeError: Invalid file types get skipped.
        """
        self.all_reports = []
        for root, dirs, files in os.walk(self.report_directory):
            for file in files:
                if file == 'accidents_report.csv':
                    print('EVENT INFO: Skipped Accident Report')
                    continue
                if file.endswith('.csv'):
                    report_type = file.replace('.csv', '').upper()
                    self.all_reports.append(self.read_csv_to_dataframe(f'{root}{file}', report_type))
                else:
                    raise TypeError(f'EVENTS ERROR: {file} is an invalid file type. Skipping this File.')

    def combine_all_dataframes(self) -> pd.DataFrame:
        """
            Combines all dataframes ready by read_all_reports. Fills all empty cells with 0. 

        Returns:
            pd.DataFrame: Combined dataframe.
        """
        dataframe = pd.concat(self.all_reports)
        dataframe = dataframe.fillna(0)
        return dataframe

    def create_final_report(self) -> pd.DataFrame:
        """Reformats base_report into a cleaner and more refined Report for final use.

        Returns:
            pd.DataFrame: Formatted Dataframe
        """
        drivers = self.base_report.groupby('Employee ID')
        rows = []
        for i, driver in drivers:
            driver_id = i
            driver_fleet = driver['Group'].unique()[0]
            start_date = self.start_date
            end_date = self.end_date
            driver_row = {
                'DRIVER': driver_id,
                'FLEET': driver_fleet,
                'START DATE': start_date,
                'END DATE': end_date,
                'HANDHELD DEVICE': driver['HANDHELD'].sum(),
                'INATTENTIVE': driver['INATTENTIVE'].sum(),
                'FOLLOWING DISTANCE': driver['FOLLOWING_DISTANCE'].sum(),
                'LANE DEPARTURE': driver['LANE_DEPARTURE'].sum(),
                'ROLLING STOP': driver['ROLLING_STOP'].sum(),
                'CRITICAL DISTANCE': driver['CRITICAL_DISTANCE'].sum(),
            }
            rows.append(driver_row)
        return pd.DataFrame(rows)

    def final_report_to_csv(self, save_path: str=None):
        """Creates CSV of final report. If save_path is provided the file will be saved there. If not it will be saved in current directory.
        Args:
            save_path (str, optional): Location you would like to save the new CSV to. Defaults to None.
        """
        file_name = f'lytx_report_{self.start_date}.csv'
        print(f'<-- Saving Report to {os.getcwd()}\lytx_report_{self.start_date}.csv -->')
        if save_path:
            self.final_report.to_csv(f'{save_path}{file_name}', index=False, header=False)
        else:
            self.final_report.to_csv(file_name, index=False, header=False)

class AccidentReport(Report):
    def __init__(self, report_directory, accidents_csv:str='accidents_report.csv', start_date: datetime=get_month_start(), end_date: datetime=get_month_end()):
        super().__init__(report_directory, start_date, end_date) 
        self.accidents_csv = accidents_csv
        print('ACCIDENT INFO: Verifying Accident Report Exists')
        try:
            self.find_file()
        except FileNotFoundError as e:
            print(e)
        print('ACCIDENT INFO: Creating base Accidents Report')
        self.base_accidents_csv = self.read_file_to_dataframe()
        print('ACCIDENT INFO: Creating Final Accidents Report')
        self.final_report = self.create_final_report()

    def find_file(self):
        if os.path.isfile(f'{self.report_directory}{self.accidents_csv}'):
            print('ACCIDENT INFO: Accident File Found')
            return
        else:
            raise FileNotFoundError(f'ACCIDENT ERROR: {self.accidents_csv} not found in reports directory')

    def read_file_to_dataframe(self) -> pd.DataFrame:
        dataframe = pd.read_csv(f'{self.report_directory}{self.accidents_csv}')
        dataframe = dataframe[['Driver', 'Accident date', 'Preventable']]
        dataframe = dataframe.rename(columns={'Driver': 'DRIVER'})
        dataframe['Accident date'] = pd.to_datetime(dataframe['Accident date']).dt.date
        dataframe['Preventable'] = dataframe['Preventable'].replace({'Yes': True, 'No': False})

        return dataframe
    
    def create_final_report(self):
        total_preventable_accidents = self.base_accidents_csv.loc[self.base_accidents_csv['Preventable'] == True]
        driver_dataframes = []
        drivers = total_preventable_accidents.groupby('DRIVER')
        for driver_code, driver in drivers:
            month_accidents = driver.loc[(driver['Accident date'] < self.end_date) & (driver['Accident date'] > self.start_date)]
            driver_dataframes.append(
                {
                    'DRIVER': driver_code,
                    'ACCIDENTS THIS MONTH': len(month_accidents),
                    'TOTAL ACCIDENTS': len(driver)
                })
        dataframe = pd.DataFrame(driver_dataframes)
        return dataframe

report = EventsReport(f'{os.getcwd()}/reports/')
accident_report = AccidentReport(f'{os.getcwd()}/reports/')