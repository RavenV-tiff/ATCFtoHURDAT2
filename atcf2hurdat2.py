import pandas as pd
import datetime as dt
from io import StringIO

# Reading the ATCF data
df = pd.read_csv('input.txt', engine="python", sep=",+", names=[
                "BASIN", "CY", "YYYYMMDDHH", "TECHNUM", "TECH", "TAU",
                "LAT", "LON", "VMAX", "MSLP", "TY",
                "RAD", "WINDCODE", "RAD1", "RAD2", "RAD3", "RAD4",
                "POUTER", "ROUTER", "RMW", "GUSTS", "EYE", "SUBREGION",
                "MAXSEAS", "INITIALS", "DIR", "SPEED", "STORMNAME", "DEPTH",
                "SEAS", "SEASCODE", "SEAS1", "SEAS2", "SEAS3", "SEAS4",
                "USERDEFINE1", "userdata1",
                "USERDEFINE2", "userdata2",
                "USERDEFINE3", "userdata3",
                "USERDEFINE4", "userdata4",
                "USERDEFINE5", "userdata5",
            ],
            converters={
                "YYYYMMDDHH": lambda d: dt.datetime(
                    int(d[1:5]), int(d[5:7]), int(d[7:9]), int(d[9:11])),
                "TAU": lambda d: dt.timedelta(hours=int(d)),
                "LAT": lambda d: (-.1 if 'S' in d else .1) * float(d.strip("NS ")),
                "LON": lambda d: (-.1 if 'W' in d else .1) * float(d.strip("WE ")),
            },
            dtype={
                "BASIN": str,
                "CY": int,
                "VMAX": float,
                "MSLP": float,
                "TY": str,
                "RAD": float,
                "RAD1": float,
                "RAD2": float,
                "RAD3": float,
                "RAD4": float,
                "ROUTER": float,
                "RMW": float,
            })

# Remove entries with RAD > 64 (to include only 34, 50, and 64 knot radii)
df = df[df['RAD'].isin([34, 50, 64, 0])]

# Format the date and time for HURDAT2
df['DATE'] = df['YYYYMMDDHH'].dt.strftime("%Y%m%d")
df['TIME'] = df['YYYYMMDDHH'].dt.strftime("%H%M")

# Group by storm and timestamp
grouped = df.groupby(['BASIN', 'YYYYMMDDHH'])

# Get the storm name from the last row
storm_name = df['STORMNAME'].iloc[-1].strip()

# Use StringIO to buffer the output
output_buffer = StringIO()

for (basin, timestamp), group in grouped:
    # Extract the year from the timestamp
    year = timestamp.strftime("%Y")
    # Formatted date and time
    date = timestamp.strftime("%Y%m%d")
    time = timestamp.strftime("%H%M")
    cy = df['CY'].iloc[-1]

    # Initialize radii values
    radii_34 = [0, 0, 0, 0]
    radii_50 = [0, 0, 0, 0]
    radii_64 = [0, 0, 0, 0]

    for _, row in group.iterrows():
        if row['RAD'] == 34:
            radii_34 = [int(row['RAD1']), int(row['RAD2']), int(row['RAD3']), int(row['RAD4'])]
        elif row['RAD'] == 50:
            radii_50 = [int(row['RAD1']), int(row['RAD2']), int(row['RAD3']), int(row['RAD4'])]
        elif row['RAD'] == 64:
            radii_64 = [int(row['RAD1']), int(row['RAD2']), int(row['RAD3']), int(row['RAD4'])]

    # Write each row entry to buffer
    entry = (
        f"{date}, {time:>4},  ,{row['TY']:>2}, "
        f"{abs(round(row['LAT'], 1)):4.1f}{'N' if row['LAT'] > 0 else 'S'}, "
        f"{abs(round(row['LON'], 1)):5.1f}{'E' if row['LON'] > 0 else 'W'}, "
        f"{int(row['VMAX']):3d}, {int(row['MSLP']):4d}, "
        f"{radii_34[0]:4d}, {radii_34[1]:4d}, {radii_34[2]:4d}, {radii_34[3]:4d}, "
        f"{radii_50[0]:4d}, {radii_50[1]:4d}, {radii_50[2]:4d}, {radii_50[3]:4d}, "
        f"{radii_64[0]:4d}, {radii_64[1]:4d}, {radii_64[2]:4d}, {radii_64[3]:4d}, "
        f"   JTWC,\n"
    )
    output_buffer.write(entry)

# Convert buffer content to string
output_content = output_buffer.getvalue()

# Count number of lines (entries) in the output content
num_entries = output_content.count('\n')

# Open the output file and write header followed by buffered content
with open('out.txt', 'w') as file:
    # Write header first
    header = f"{basin}{cy:02d}{year}, {storm_name:>18}, {num_entries:>6},\n"
    file.write(header)
    # Write buffered content
    file.write(output_content)