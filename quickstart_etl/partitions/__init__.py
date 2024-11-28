from dagster import DailyPartitionsDefinition, WeeklyPartitionsDefinition, MonthlyPartitionsDefinition
from ..assets import constants

start_date = constants.START_DATE
end_date = constants.END_DATE

daily_partition = DailyPartitionsDefinition(
    start_date=start_date,
    end_date=end_date
)

weekly_partition = WeeklyPartitionsDefinition(
    start_date=start_date,
    end_date=end_date
)

monthly_partition = MonthlyPartitionsDefinition(
    start_date=start_date,
    end_date=end_date
)
