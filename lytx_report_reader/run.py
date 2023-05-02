from datetime import datetime, timedelta
import os

import pandas as pd

def get_month_start(date: datetime=datetime.today()):
    month_start = (date - timedelta(days=date.day)).replace(day=1).date()
    return month_start

def get_month_end(date: datetime=datetime.today()):
    month_end = (date - timedelta(days=date.day)).date()
    return month_end

class EventsReport:
    def __init__(self, start_date: datetime=get_month_start(), end_date: datetime=get_month_end(), create_file: bool=True):
        self.start_date = start_date
        self.end_date = end_date
        self.directory = f'{os.getcwd()}/reports/'
        print('<-- Reading Individual Reports -->')
        try:
            self.read_all_reports()
        except TypeError as e:
            print(e)
        print('<-- Combining Reports -->')
        self.base_report = self.combine_all_dataframes()
        print('<-- Creating Final Report -->')
        self.final_report = self.create_final_report()
        if create_file:
            print(f'<-- Saving Report to {os.getcwd()}\lytx_report_{start_date}.csv -->')
            self.final_report.to_csv(f'lytx_report_{start_date}.csv')

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
        for root, dirs, files in os.walk(self.directory):
            for file in files:
                if file.endswith('.csv'):
                    report_type = file.replace('.csv', '').upper()
                    self.all_reports.append(self.read_csv_to_dataframe(f'{root}{file}', report_type))
                else:
                    print(f'Skipped {file}...')
                    raise TypeError(f'{file} is an invalid file type')

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

report = EventsReport(create_file=False)

# print(pd.read_csv('accidents.csv'))