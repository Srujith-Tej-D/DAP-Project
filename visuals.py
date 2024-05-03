from dagster import op, job 
import pandas as pd
from sqlalchemy import create_engine
import plotly.graph_objects as go
import plotly.express as px

@op
def analyze_traffic_data():
    # SQLAlchemy connection
    engine = create_engine('postgresql://postgres:srudap@localhost:5432/postgres')

    # Crash Data Query
    crash_query = """
    SELECT
        "CRASH_PLACE" AS place,
        COUNT(*) AS total_crashes
    FROM
        datatable
    WHERE
        "CRASH_PLACE" IS NOT NULL
    GROUP BY
        "CRASH_PLACE"
    ORDER BY
        total_crashes DESC
    LIMIT 5;
    """

    # Violation Data Query
    violation_query = """
    SELECT
        "VIOLATION_PLACE" AS place,
        COUNT(*) AS total_violations
    FROM
        datatable
    WHERE
        "VIOLATION_PLACE" IS NOT NULL
    GROUP BY
        "VIOLATION_PLACE"
    ORDER BY
        total_violations DESC
    LIMIT 5;
    """

    # Execute the queries
    crash_data = pd.read_sql_query(crash_query, engine)
    violation_data = pd.read_sql_query(violation_query, engine)

    # Lifetime Top 5 for Crashes
    lifetime_crash_fig = go.Figure()
    lifetime_crash_fig.add_trace(go.Bar(x=crash_data['place'], y=crash_data['total_crashes'], marker_color='red'))
    lifetime_crash_fig.update_layout(
        title='Top 5 Crash Locations since 2023',
        xaxis_title='Place',
        yaxis_title='Total Crashes',
        legend_title='Place'
    )
    lifetime_crash_fig.show()

    # Lifetime Top 5 for Violations
    lifetime_violation_fig = go.Figure()
    lifetime_violation_fig.add_trace(go.Bar(x=violation_data['place'], y=violation_data['total_violations'], marker_color='orange'))
    lifetime_violation_fig.update_layout(
        title='Top 5 Violation Locations since 2023',
        xaxis_title='Place',
        yaxis_title='Total Violations',
        legend_title='Place'
    )
    lifetime_violation_fig.show()

@op
def analyze_weekday_traffic():
    # SQLAlchemy connection
    engine = create_engine('postgresql://postgres:srudap@localhost:5432/postgres')

    # Execute the queries for crashes
    crash_query = """
    SELECT
        TO_CHAR("CRASH_DATE", 'Day') AS weekday,
        COUNT(*) AS total_crashes
    FROM
        datatable
    WHERE
        "CRASH_DATE" IS NOT NULL
    GROUP BY
        weekday
    ORDER BY
        CASE 
            WHEN TO_CHAR("CRASH_DATE", 'Day') = 'Monday    ' THEN 1
            WHEN TO_CHAR("CRASH_DATE", 'Day') = 'Tuesday   ' THEN 2
            WHEN TO_CHAR("CRASH_DATE", 'Day') = 'Wednesday ' THEN 3
            WHEN TO_CHAR("CRASH_DATE", 'Day') = 'Thursday  ' THEN 4
            WHEN TO_CHAR("CRASH_DATE", 'Day') = 'Friday    ' THEN 5
            WHEN TO_CHAR("CRASH_DATE", 'Day') = 'Saturday  ' THEN 6
            WHEN TO_CHAR("CRASH_DATE", 'Day') = 'Sunday    ' THEN 7
        END;
    """
    crash_data = pd.read_sql_query(crash_query, engine)

    # Execute the queries for violations
    violation_query = """
    SELECT
        TO_CHAR("VIOLATION_DATE", 'Day') AS weekday,
        COUNT(*) AS total_violations
    FROM
        datatable
    WHERE
        "VIOLATION_DATE" IS NOT NULL
    GROUP BY
        weekday
    ORDER BY
        CASE 
            WHEN TO_CHAR("VIOLATION_DATE", 'Day') = 'Monday    ' THEN 1
            WHEN TO_CHAR("VIOLATION_DATE", 'Day') = 'Tuesday   ' THEN 2
            WHEN TO_CHAR("VIOLATION_DATE", 'Day') = 'Wednesday ' THEN 3
            WHEN TO_CHAR("VIOLATION_DATE", 'Day') = 'Thursday  ' THEN 4
            WHEN TO_CHAR("VIOLATION_DATE", 'Day') = 'Friday    ' THEN 5
            WHEN TO_CHAR("VIOLATION_DATE", 'Day') = 'Saturday  ' THEN 6
            WHEN TO_CHAR("VIOLATION_DATE", 'Day') = 'Sunday    ' THEN 7
        END;
    """
    violation_data = pd.read_sql_query(violation_query, engine)

    # Color palette (one color per weekday)
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2']

    # Create 3D Pie Charts for Crashes and Violations
    fig = go.Figure(data=[
        go.Pie(labels=crash_data['weekday'].str.strip(), values=crash_data['total_crashes'], name="Crashes",
               hoverinfo='label+percent', textinfo='label+percent', marker=dict(colors=colors), 
               domain=dict(x=[0, 0.5]), textfont_size=21, pull=[0.1]*7, textposition='outside', insidetextorientation='radial'),
        go.Pie(labels=violation_data['weekday'].str.strip(), values=violation_data['total_violations'], name="Violations",
               hoverinfo='label+percent', textinfo='label+percent', marker=dict(colors=colors), 
               domain=dict(x=[0.5, 1.0]), textfont_size=21, pull=[0.1]*7, textposition='outside', insidetextorientation='radial')
    ])

    fig.update_traces(hole=0.4)

    fig.update_layout(
        title_text='Distribution of Crashes and Violations by Day of the Week',
        annotations=[dict(text='Crashes', x=0.20, y=0.5, font_size=24, showarrow=False),
                     dict(text='Violations', x=0.80, y=0.5, font_size=20, showarrow=False)]
    )

    fig.show()
@op
def analyze_road_defects():
    # Establish a database connection
    engine = create_engine('postgresql://postgres:srudap@localhost:5432/postgres')

    # Execute the SQL query
    query = """
    SELECT
        "ROAD_DEFECT",
        "CRASH_PLACE",
        total_crashes
    FROM (
        SELECT
            "ROAD_DEFECT",
            "CRASH_PLACE",
            COUNT(*) AS total_crashes,
            RANK() OVER (PARTITION BY "ROAD_DEFECT" ORDER BY COUNT(*) DESC) as rank
        FROM datatable
        WHERE "ROAD_DEFECT" IN ('RUT', 'HOLES', 'WORN SURFACE') AND "ROAD_DEFECT" IS NOT NULL
        GROUP BY "ROAD_DEFECT", "CRASH_PLACE"
    ) subquery
    WHERE rank <= 5;
    """
    data = pd.read_sql_query(query, engine)

    # Prepare data for Sankey diagram
    label_list = pd.concat([data['ROAD_DEFECT'], data['CRASH_PLACE']]).unique()
    source_indices = [list(label_list).index(x) for x in data['ROAD_DEFECT']]
    target_indices = [list(label_list).index(x) for x in data['CRASH_PLACE']]

    # Create the Sankey diagram
    fig = go.Figure(data=[go.Sankey(
        node=dict(
            pad=15,
            thickness=20,
            line=dict(color="black", width=0.5),
            label=label_list,
            hovertemplate='Total %{value} crashes<extra></extra>',  # Customize hover for nodes
        ),
        link=dict(
            source=source_indices,  # indices from ROAD_DEFECT
            target=target_indices,  # indices to CRASH_PLACE
            value=data['total_crashes'],  # Number of crashes
            hovertemplate='%{source.label} to %{target.label}<br>%{value} crashes<extra></extra>',  # Customize hover for links
        )
    )])

    fig.update_layout(title_text="Sankey Diagram: Road Defects to Top 5 Crash Places", font_size=17)
    fig.show()
@op
def visualize_weather_impact_on_crashes():
    # Establish a database connection
    engine = create_engine('postgresql://postgres:srudap@localhost:5432/postgres')

    # Execute the SQL query
    query = """
    SELECT
        "WEATHER_CONDITION",
        "CRASH_PLACE",
        COUNT(*) AS total_crashes
    FROM datatable
    WHERE "WEATHER_CONDITION" IN ('RAIN', 'SNOW') AND "WEATHER_CONDITION" IS NOT NULL
        AND "CRASH_PLACE" IS NOT NULL
    GROUP BY "WEATHER_CONDITION", "CRASH_PLACE"
    ORDER BY "WEATHER_CONDITION", total_crashes DESC;
    """
    data = pd.read_sql_query(query, engine)


    # Generate a Treemap
    fig = px.treemap(data, path=['WEATHER_CONDITION', 'CRASH_PLACE'], values='total_crashes',
                     color='total_crashes', hover_data=['CRASH_PLACE'],
                     color_continuous_scale='RdBu',
                     title='Treemap of Crash Places Affected by Weather Conditions')
    fig.update_layout(margin=dict(t=50, l=25, r=25, b=25))
    fig.show()

@op
def visualize_crash_places_with_no_controls():
    # Establish a database connection
    engine = create_engine('postgresql://postgres:srudap@localhost:5432/postgres')

    # Execute the SQL query
    query = """
    SELECT
        "CRASH_PLACE",
        COUNT(*) AS total_crashes
    FROM datatable
    WHERE "TRAFFIC_CONTROL_DEVICE" = 'NO CONTROLS'
    GROUP BY "CRASH_PLACE"
    ORDER BY total_crashes DESC
    LIMIT 5;
    """
    data = pd.read_sql_query(query, engine)

    # Define an inverted red color scale (light to dark)
    red_color_scale = [
        [0, 'rgb(255, 160, 122)'],  # Light Salmon
        [0.2, 'rgb(250, 128, 114)'],  # Salmon
        [0.4, 'rgb(240, 128, 128)'],  # Light Coral
        [0.6, 'rgb(205, 92, 92)'],  # Indian Red
        [0.8, 'rgb(178, 34, 34)'],  # Firebrick
        [1, 'rgb(139, 0, 0)']   # Dark Red
    ]

    # Generate a Sunburst Chart with enhanced labeling
    fig = px.sunburst(
        data, 
        path=['CRASH_PLACE'], 
        values='total_crashes',
        color='total_crashes',
        color_continuous_scale=red_color_scale,
        title='Top 5 Crash Places with No Traffic Controls',
        hover_data={'total_crashes': True}  # Ensure hover shows the total crashes
    )

    # Update layout and labels
    fig.update_traces(textinfo='label+percent entry')
    fig.update_layout(
        margin=dict(t=50, l=25, r=25, b=25),
        coloraxis_colorbar=dict(
            title='Total Crashes',
            tickvals=[data['total_crashes'].min(), data['total_crashes'].max()],
            ticktext=['Less Frequent', 'More Frequent']
        )
    )

    # Show the plot
    fig.show()

@op
def analyze_speed_limits_by_weekday():
    # Establish a database connection
    engine = create_engine('postgresql://postgres:srudap@localhost:5432/postgres')

    # Execute the SQL query
    query = """
    SELECT
        TO_CHAR("CRASH_DATE", 'Day') AS weekday,
        AVG("POSTED_SPEED_LIMIT") AS avg_speed_limit
    FROM
        datatable
    WHERE
        "CRASH_DATE" IS NOT NULL
    GROUP BY
        weekday
    ORDER BY
        CASE 
            WHEN TO_CHAR("CRASH_DATE", 'Day') = 'Monday    ' THEN 1
            WHEN TO_CHAR("CRASH_DATE", 'Day') = 'Tuesday   ' THEN 2
            WHEN TO_CHAR("CRASH_DATE", 'Day') = 'Wednesday ' THEN 3
            WHEN TO_CHAR("CRASH_DATE", 'Day') = 'Thursday  ' THEN 4
            WHEN TO_CHAR("CRASH_DATE", 'Day') = 'Friday    ' THEN 5
            WHEN TO_CHAR("CRASH_DATE", 'Day') = 'Saturday  ' THEN 6
            WHEN TO_CHAR("CRASH_DATE", 'Day') = 'Sunday    ' THEN 7
        END;
    """
    data = pd.read_sql_query(query, engine)

    # Create a bar chart for the average posted speed limits by weekday
    fig = go.Figure(go.Bar(
        x=data['weekday'].str.strip(),  # Ensure that extra spaces are removed from weekdays
        y=data['avg_speed_limit'],
        marker_color='indianred'  # Color of the bars
    ))

    fig.update_layout(
        title='Average Posted Speed Limit by Day of the Week',
        xaxis_title='Day of the Week',
        yaxis_title='Average Posted Speed Limit',
        yaxis=dict(range=[min(data['avg_speed_limit'])-5, max(data['avg_speed_limit'])+5])  # Adjusting the y-axis range for better visualization
    )

    # Show the plot
    fig.show()

@op
def visualize_violations_by_weekday():
    # Establish a database connection
    engine = create_engine('postgresql://postgres:srudap@localhost:5432/postgres')

    # Execute the SQL query
    violation_query = """
    SELECT
        TO_CHAR("VIOLATION_DATE", 'Day') AS weekday,
        COUNT(*) AS total_violations
    FROM
        datatable
    WHERE
        "VIOLATION_DATE" IS NOT NULL
    GROUP BY
        weekday
    ORDER BY
        CASE 
            WHEN TO_CHAR("VIOLATION_DATE", 'Day') = 'Monday    ' THEN 1
            WHEN TO_CHAR("VIOLATION_DATE", 'Day') = 'Tuesday   ' THEN 2
            WHEN TO_CHAR("VIOLATION_DATE", 'Day') = 'Wednesday ' THEN 3
            WHEN TO_CHAR("VIOLATION_DATE", 'Day') = 'Thursday  ' THEN 4
            WHEN TO_CHAR("VIOLATION_DATE", 'Day') = 'Friday    ' THEN 5
            WHEN TO_CHAR("VIOLATION_DATE", 'Day') = 'Saturday  ' THEN 6
            WHEN TO_CHAR("VIOLATION_DATE", 'Day') = 'Sunday    ' THEN 7
        END;
    """
    violation_data = pd.read_sql_query(violation_query, engine)

    # Define colors for each day of the week
    colors = ['#FFD700', '#FFA07A', '#1E90FF', '#32CD32', '#FF4500', '#6A5ACD', '#FF69B4']

    # Create Pie Chart for Violations
    fig = go.Figure(go.Pie(
        labels=violation_data['weekday'].str.strip(), 
        values=violation_data['total_violations'],
        hoverinfo='label+percent',
        textinfo='label+percent',
        marker=dict(colors=colors),
        pull=[0.02] * len(violation_data)  # Optional: slightly separate slices for better visibility
    ))

    fig.update_layout(
        title='Distribution of Violations by Day of the Week',
        autosize=False,  # Disable autosize to manually set dimensions
        width=800,       # Width of the figure in pixels
        height=600       # Height of the figure in pixels
    )

    # Show the plot
    fig.show()

@job

def visualizations():
    analyze_road_defects()
    visualize_weather_impact_on_crashes()
    visualize_crash_places_with_no_controls()
    analyze_speed_limits_by_weekday()
    visualize_violations_by_weekday()
    analyze_traffic_data()
    analyze_weekday_traffic()
    

    
