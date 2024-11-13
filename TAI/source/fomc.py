import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import calendar
import re
import io


class FOMC:
    def __init__(self, start_year=2024):
        self.start_year = start_year
        # Expected FOMC meeting months pattern
        self.meeting_months = [1, 3, 4, 6, 7, 9, 11, 12]
        self.df = None
        self.federal_funds_df = None

    def scrape_meeting_dates(self):
        """Scrapes FOMC meeting dates from the Federal Reserve website."""
        url = "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"
        response = requests.get(url)
        soup = BeautifulSoup(response.content, "html.parser")

        dates = []
        for item in soup.select(".fomc-meeting__date"):
            dates.append(item.get_text(strip=True))

        # Convert list to DataFrame
        self.df = pd.DataFrame(dates, columns=["Meeting Date"])

    def process_dates(self):
        """Processes the scraped meeting dates to handle cross-month dates and format them."""
        current_year = self.start_year
        year_dates = []

        for i, date in enumerate(self.df["Meeting Date"]):
            month = self.meeting_months[i % len(self.meeting_months)]

            # Extract numeric days only, ignoring any extra text like '(unscheduled)'
            days = re.findall(r'\d+', date)

            # Skip if no valid date is found
            if not days:
                year_dates.append("Invalid Date")
                continue

            # Calculate the start date, adjusting if needed for the last day of the month
            start_day = int(days[0])
            last_day_of_month = calendar.monthrange(current_year, month)[1]
            start_day = min(start_day, last_day_of_month)
            start_date = datetime(current_year, month, start_day)

            # Handle end date, including cross-month logic
            if len(days) > 1:
                end_day = int(days[1])
                if end_day < start_day:
                    next_month = month + 1 if month < 12 else 1
                    next_year = current_year if next_month > 1 else current_year + 1
                    last_day_of_next_month = calendar.monthrange(
                        next_year, next_month)[1]
                    end_day = min(end_day, last_day_of_next_month)
                    end_date = datetime(next_year, next_month, end_day)
                else:
                    end_day = min(end_day, last_day_of_month)
                    end_date = datetime(current_year, month, end_day)

                formatted_date = f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
            else:
                formatted_date = start_date.strftime('%Y-%m-%d')

            year_dates.append(formatted_date)

            # Increment the year if December has been reached
            if month == 12:
                current_year += 1

        # Add formatted dates and split into 'From Date' and 'To Date'
        self.df["Formatted Meeting Date"] = year_dates
        self.df[['From Date', 'To Date']] = self.df["Formatted Meeting Date"].str.split(
            ' to ', expand=True)

        # Drop the 'Formatted Meeting Date' column if no longer needed
        self.df = self.df.drop(columns=["Formatted Meeting Date"])

    def get_dataframe(self):
        """Returns the processed DataFrame of FOMC meeting dates."""
        if self.df is not None:
            return self.df
        else:
            raise ValueError(
                "DataFrame is empty. Run scrape_meeting_dates() and process_dates() first.")

    def get_federal_funds_rate(self, from_date="1960-01-01", to_date=None):
        """Downloads and processes historical federal funds rates by date from the Federal Reserve's website."""
        if to_date is None:
            to_date = datetime.now().strftime("%Y-%m-%d")

        # Format dates for the URL
        from_date_formatted = datetime.strptime(
            from_date, "%Y-%m-%d").strftime("%m/%d/%Y")
        to_date_formatted = datetime.strptime(
            to_date, "%Y-%m-%d").strftime("%m/%d/%Y")

        # Construct the URL with the specified date range
        url = (
            f"https://www.federalreserve.gov/datadownload/Output.aspx?rel=H15&series=8e83f7f17c5cea4d190d85ae6737639f"
            f"&from={from_date_formatted}&to={to_date_formatted}&filetype=csv&label=include&layout=seriescolumn"
        )

        # Set headers to mimic a browser request
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36"
        }

        # Download and load CSV data into DataFrame
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()  # Raise an error if the request was unsuccessful
            federal_funds_df = pd.read_csv(
                io.StringIO(response.text), skiprows=5)
            print("Federal funds rate data downloaded successfully.")
        except Exception as e:
            print(f"Failed to download data: {e}")
            return None

        # Rename and drop unnecessary columns
        federal_funds_df.rename(
            columns={"Time Period": "date",
                     "RIFSPFF_N.WW": "federal_fund_rate"},
            inplace=True
        )
        federal_funds_df.drop(
            columns=["RIFSPBLP_N.WW", "RIFSRP_F02_N.WW"], inplace=True, errors="ignore")

        # Convert the date column to datetime format
        federal_funds_df["date"] = pd.to_datetime(
            federal_funds_df["date"], errors="coerce")

        self.federal_funds_df = federal_funds_df  # Store the processed DataFrame

    def get_federal_funds_dataframe(self):
        """Returns the processed DataFrame of federal funds rates."""
        if self.federal_funds_df is not None:
            return self.federal_funds_df
        else:
            raise ValueError(
                "Federal funds DataFrame is empty. Run get_federal_funds_rate() first.")

    def to_json(self, df_type="meeting"):
        """Converts the selected DataFrame (meeting dates or federal funds rate) to JSON format."""
        df = self.df if df_type == "meeting" else self.federal_funds_df
        if df is not None:
            return df.to_json(orient="records", date_format="iso")
        else:
            raise ValueError(
                "Selected DataFrame is empty. Run the appropriate method first.")


if __name__ == "__main__":
    # Example usage
    scraper = FOMC(start_year=2024)
    scraper.scrape_meeting_dates()
    scraper.process_dates()

    # Get meeting dates DataFrame
    df_meetings = scraper.get_dataframe()
    print("FOMC Meeting Dates:")
    print(df_meetings)

    # Get federal funds rate DataFrame for a specific range
    scraper.get_federal_funds_rate(
        from_date="2000-01-01", to_date="2024-11-13")
    df_federal_funds = scraper.get_federal_funds_dataframe()
    print("Federal Funds Rate Data:")
    print(df_federal_funds)

    # Convert meeting dates and federal funds rate DataFrames to JSON
    meetings_json = scraper.to_json(df_type="meeting")
    federal_funds_json = scraper.to_json(df_type="federal_funds")
    print("FOMC Meeting Dates JSON:")
    print(meetings_json)
    print("Federal Funds Rate JSON:")
    print(federal_funds_json)
