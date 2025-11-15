from datetime import date, timedelta

def get_previous_week_dates(today_date=None):
    if today_date is None:
        today_date = date.today()

    # Calculate the start of the current week (Monday)
    # isoweekday() returns 1 for Monday, 7 for Sunday.
    # Subtracting (today_date.isoweekday() - 1) gives the number of days to subtract to reach Monday.
    current_week_start = today_date - timedelta(days=today_date.isoweekday() - 1)

    # Calculate the start of the previous week
    previous_week_start = current_week_start - timedelta(weeks=1)

    # Generate all dates in the previous week
    previous_week_dates = []
    for i in range(7):
        previous_week_dates.append(previous_week_start + timedelta(days=i))

    return previous_week_dates

# Example with a specific date
specific_date = date(2025, 11, 5) # A Monday
previous_week_from_specific_date = get_previous_week_dates(specific_date)
print(f"Dates of the week before {specific_date}: {previous_week_from_specific_date}")