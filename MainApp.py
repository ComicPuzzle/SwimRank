import math
import time
from nicegui import Client, ui, app
import pandas as pd
import asyncio
import asyncpg
from nicegui.events import KeyEventArguments
from datetime import datetime, timedelta
from get_credentials import get_credentials
import qrcode
from io import BytesIO

# --- GLOBAL DB POOL ---
global_pool = None
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

def navbar():
    bg_color = 'bg-gray-200/70'
    
    with ui.row().classes(f'w-full {bg_color} py-4 justify-center items-center flex-wrap md:flex-nowrap shadow-sm'):
        # Home button
        ui.button('Home', on_click=lambda: ui.navigate.to('/')).props('flat').classes('text-2xl font-semibold hover:shadow-md transition-shadow duration-200')

        # Separator (hidden on small screens)
        ui.label('|').classes('text-2xl text-gray-500 sm:inline-block')

        # Rankings button
        ui.button('Rankings', on_click=lambda: ui.navigate.to("/rankings")).props('flat').classes('text-2xl font-semibold hover:shadow-md transition-shadow duration-200')

        ui.label('|').classes('text-2xl text-gray-500 sm:inline-block')

        # Discussion button
        ui.button('Discussion', on_click=lambda: ui.navigate.to('/discussion')).props('flat').classes('text-2xl font-semibold hover:shadow-md transition-shadow duration-200')
        
        ui.label('|').classes('text-2xl text-gray-500 sm:inline-block')

        # Discussion button
        ui.button('About Me', on_click=lambda: ui.navigate.to('/aboutme')).props('flat').classes('text-2xl font-semibold hover:shadow-md transition-shadow duration-200')
        
        ui.label('|').classes('text-2xl text-gray-500 sm:inline-block')

        # Discussion button
        ui.button('Privacy Policy/FAQ', on_click=lambda: ui.navigate.to('/privacy')) \
            .props('flat') \
            .classes('text-2xl font-semibold hover:shadow-md transition-shadow duration-200')
        
        ui.label('|').classes('text-2xl text-gray-500 sm:inline-block')

        # Discussion button
        ui.button('Donate', on_click=lambda: ui.navigate.to('/donate')) \
            .props('flat') \
            .classes('text-2xl font-semibold hover:shadow-md transition-shadow duration-200')

async def get_global_pool():
    """Return a shared asyncpg pool for all requests."""
    dbname, port, password, ip = get_credentials()
    global global_pool
    if global_pool is None or global_pool.is_closing():
        global_pool = await asyncpg.create_pool(
            dsn=f'postgres://postgres:{password}@{ip}:{port}/{dbname}', #change to remote address
            max_inactive_connection_lifetime=20,
            min_size=1,
            max_size=10,  # adjust based on server capacity
        )
    return global_pool

def get_current_season():
    session = app.storage.tab
    session['current_month'] = datetime.now().month
    session['current_year'] = datetime.now().year
    if session['current_month'] >= 9:
        session['current_season'] = f"{'9/01/' + str(session['current_year']) + ' - 8/31/' + str(session['current_year'] + 1)}"
    else:
        session['current_season'] = f"{'9/01' + str(session['current_year'] - 1) + ' - 8/31/' + str(session['current_year'])}"
    return session['current_season']
@app.on_shutdown
async def shutdown():
    """Close global pool on app shutdown."""
    global global_pool
    if global_pool and not global_pool.is_closing():
        await global_pool.close()
    
def get_age_group(age):
    a1 = 0
    a2 = 10
    if age <= 10:
        pass
    elif age <= 12:
        a1, a2 = 11, 12
    elif age <= 14:
        a1, a2 = 13, 14
    elif age <= 16:
        a1, a2 = 15, 16
    elif age <= 18:
        a1, a2 = 17, 18
    else:
        a1, a2 = 19, 99
    return (a1, a2)

def age_group_str(age):
    return f"{age[0]}-{age[1]}"

def convert_timedelta(val):
    minutes = int(val.total_seconds() // 60)
    if minutes == 0:
        seconds = val.total_seconds()
        return f"{round(seconds, 2):.2f}"
    else:
        seconds = val.total_seconds() - 60 * minutes
        return f"{minutes}:{round(seconds, 2):05.2f}"

def str_to_datetime(val):
    if ":" in val:
        date_format = "%M:%S.%f"
    else:
        date_format = "%S.%f"
    return datetime.strptime(val, date_format)

def str_to_timedelta(t_str):
    parts = t_str.split(":")
    if len(parts) == 2:
        minutes = int(parts[0])
        seconds = float(parts[1])
        total_seconds = minutes * 60 + seconds
    else:
        total_seconds = float(parts[0])
    return timedelta(seconds=total_seconds)

async def handle_key(e: KeyEventArguments):
    await ui.context.client.connected()
    session = app.storage.tab
    if e.modifiers.ctrl and e.action.keydown:
        session['control_timer'] = time.time()
    elif e.key == 'c' and e.action.keyup:
        if time.time() - session.get('control_timer', 0) < 0.5:
            app.shutdown()  # Stop the NiceGUI application

async def fetch_people(name):
    name = name.lower().strip().split()
    name[0] = name[0][0].upper() + name[0][1:]
    name[1] = name[1][0].upper() + name[1][1:]
    session = app.storage.tab

    pool = await get_global_pool()
    async with pool.acquire() as con:
        query = """
            SELECT *
            FROM "ResultsSchema"."SwimmerIDs"
            WHERE "FirstName" = $1
            AND "LastName"  = $2"""
        rows = await con.fetch(query, name[0], name[1])

    session['id_table_df'] = pd.DataFrame(rows, columns=['FirstName', 'MiddleName', 'LastName', 'Team', 'LSC', 'Age', 'Sex', 'PersonKey'])
    session['id_table_df'].insert(loc=0, column='Name', value='')
    session['id_table_df']['Name'] = (session['id_table_df'][['FirstName', 'MiddleName', 'LastName']]
                                            .fillna('')                 # replace None/NaN with empty string
                                            .agg(' '.join, axis=1)      # join with spaces
                                            .str.replace(r'\s+', ' ', regex=True)  # collapse double spaces
                                            .str.strip()                # remove leading/trailing spaces
                                        )
    session['id_table_df'].drop(columns=['FirstName', 'MiddleName', 'LastName'], inplace=True)
    await update_id_table()

async def fetch_team_swimmers(team: str):
    pool = await get_global_pool()
    async with pool.acquire() as con:
        rows = await con.fetch(
            '''
            SELECT *
            FROM "ResultsSchema"."SwimmerIDs"
            WHERE "Team" = $1
            ORDER BY "LastName", "FirstName"
            ''',
            team,
        )

    df = pd.DataFrame([dict(r) for r in rows])
    return df

async def fetch_person_event_data(table, key):
    query = f"""SELECT "Event", "Sex", "SwimTime", "Relay", "Age", "AgeGroup", "Points", "TimeStandard", "LSC", 
                            "Meet", "Team", "SwimDate", "national_rank", "lsc_rank", "team_rank" FROM "ResultsSchema"."{table}" WHERE "PersonKey" = {key}"""
    pool = await get_global_pool()
    async with pool.acquire() as con:
        rows = await con.fetch(query)
    return rows

async def fetch_ranking_data(table1, table2, age_group, sex, season):
    start_str, end_str = season.split(" - ")
    season_start = datetime.strptime(start_str, "%m/%d/%Y").strftime("%Y-%m-%d")
    season_end = datetime.strptime(end_str, "%m/%d/%Y").strftime("%Y-%m-%d")
    def add_query(table, age_group, sex):
        if table == None:
            return f""""""
        else:
            query = f"""SELECT "Event", "Name", "Sex", "PersonKey", "Age", "LSC", "Team", "SwimTime", "national_rank", "lsc_rank", "team_rank" FROM  "ResultsSchema"."{table}" 
                        WHERE "national_rank" != -1 AND "SwimDate" >= '{season_start}' AND "SwimDate" < '{season_end}' AND "AgeGroup" = '{age_group}' AND "Sex" = {sex} """
            
            return query
        
    query = add_query(table1, age_group, sex)
    query = query + " UNION ALL " + add_query(table2, age_group, sex)
    query += """ORDER BY "national_rank" ASC"""
    pool = await get_global_pool()
    async with pool.acquire() as con:
        rows = await con.fetch(query)
    return rows

async def fetch_ncaa_comp_data(time, gender, event):
    if gender == "Male":
        tables = ["DivI_Male",  "DivII_Male",  "DivIII_Male"]
    else:
        tables = ["DivI_Male",  "DivII_Male",  "DivIII_Male"]
    query = f"""SELECT 100.0 * COUNT(*) FILTER (WHERE EXTRACT(EPOCH FROM "SwimTime") > {time}) / COUNT(*) AS pct_faster
                FROM "ResultsSchema"."{tables[0]}"
                WHERE "Event" = '{event}'

                UNION ALL

                SELECT 100.0 * COUNT(*) FILTER (WHERE EXTRACT(EPOCH FROM "SwimTime") > {time}) / COUNT(*) AS pct_faster
                FROM "ResultsSchema"."{tables[1]}"
                WHERE "Event" = '{event}'

                UNION ALL

                SELECT 100.0 * COUNT(*) FILTER (WHERE EXTRACT(EPOCH FROM "SwimTime") > {time}) / COUNT(*) AS pct_faster
                FROM "ResultsSchema"."{tables[2]}"
                WHERE "Event" = '{event}';
                """
    pool = await get_global_pool()
    async with pool.acquire() as con:
        rows = await con.fetch(query)
    return rows

async def collect_all_event_data(person_key):
    db_table_names = ['50_FR_SCY_results', '50_FR_LCM_results', '100_FR_SCY_results', '100_FR_LCM_results',
                        '200_FR_SCY_results', '200_FR_LCM_results', '400_FR_LCM_results', '500_FR_SCY_results', 
                        '800_FR_LCM_results', '1000_FR_SCY_results', '1500_FR_LCM_results', '1650_FR_SCY_results',
                        '50_BK_SCY_results', '100_BK_SCY_results', '200_BK_SCY_results', '50_BK_LCM_results', '100_BK_LCM_results', '200_BK_LCM_results',
                        '50_FL_SCY_results', '100_FL_SCY_results', '200_FL_SCY_results', '50_FL_LCM_results', '100_FL_LCM_results', '200_FL_LCM_results',
                        '50_BR_SCY_results', '100_BR_SCY_results', '200_BR_SCY_results', '50_BR_LCM_results', '100_BR_LCM_results', '200_BR_LCM_results',
                        '100_IM_SCY_results', '200_IM_SCY_results', '400_IM_SCY_results', '200_IM_LCM_results', '400_IM_LCM_results'
                        ]
    #Person event data
    tasks = []
    for table in db_table_names:
        tasks.append(fetch_person_event_data(table, person_key))
    results = await asyncio.gather(*tasks)
    all_event_data = [item for sublist in results if sublist for item in sublist]
    return all_event_data

async def update_id_table():
    await ui.context.client.connected()
    session = app.storage.tab
    session['id_table'].columns = [{'name': col, 'label': col, 'field': col} for col in ['Name', 'Team', 'LSC', 'Age', 'Sex']]
    temp = session['id_table_df'].copy()
    def alter_sex(x):
        if x == 0:
            return "Male"
        elif x == 1:
            return "Female"
        else:
            return "None"
    temp['Sex'] = temp['Sex'].apply(lambda x: alter_sex(x))
    session['id_table'].rows = temp.to_dict('records')

    session['id_table'].add_slot('body-cell-Name', """
        <q-td :props="props">
            <q-btn @click="() => $parent.$emit('person_selected', props.row)" 
                    :label="props.row.Name" 
                    flat dense color='primary'/>
        </q-td>
    """)
    session['id_table'].update()
    session['id_table'].visible = True

    def on_person_selected(msg):
        person = msg.args 
        session['person'] = person
        ui.navigate.to(f'/swimmer/{person["PersonKey"]}')

    session['id_table'].on('person_selected', on_person_selected)

async def update_progression_chart():
    # 1) prepare SCY series
    await ui.context.client.connected()
    session = app.storage.tab
    def get_series(df):
        df_copy = df.copy()
        df_copy['parsed_time'] = df_copy['SwimTime'].apply(lambda x: str_to_timedelta(x.replace("r", "")))
        df_copy['parsed_date'] = pd.to_datetime(df_copy['SwimDate'], format='%m/%d/%Y')
        df_copy.sort_values('parsed_date', inplace=True)

        return df_copy.apply(
            lambda row: [row['parsed_date'].timestamp() * 1000,
                        row['parsed_time'].total_seconds()],
            axis=1
        ).tolist()


    series = []
    if not session['scy_df'].empty:
        scy_series = get_series(session['scy_df'])
        series.append({
                'name': 'SCY',
                'type': 'line',
                'data': scy_series,
                'smooth': False,
                'itemStyle': { 'color': 'blue' },
                'lineStyle': { 'width': 3 },
                'symbolSize': 6,
            })
    else:
        scy_series = []
    if not session['lcm_df'].empty:
        lcm_series = get_series(session['lcm_df'])
        series.append({
                'name': 'LCM',
                'type': 'line',
                'data': lcm_series,
                'smooth': False,
                'itemStyle': { 'color': 'red' },
                'lineStyle': { 'width': 3 },
                'symbolSize': 6,
            })
    else:
        lcm_series = []
    all_times = [point[1] for point in scy_series + lcm_series]
    min_time = min(all_times)
    min_with_buffer = math.floor(min_time * 0.8 / 5) * 5
    
    option = {
        'legend': { 'data': ['SCY', 'LCM'] },
        'tooltip': {
            'trigger': 'axis',
            ':formatter': """function(params) {
                function formatTime(seconds) {
                    const min = Math.floor(seconds / 60);
                    const sec = (seconds % 60).toFixed(2).padStart(5, '0');
                    return min > 0 ? `${min}:${sec}` : sec;
                }
                return params.map(p => 
                    p.seriesName + ' (' + new Date(p.value[0]).toLocaleDateString() + '): ' + formatTime(p.value[1])
                ).join('<br/>');
            }"""
            },
        'xAxis': {
            'type': 'time',
            'name': 'Date',
        },
        'yAxis': {
            'type': 'value',
            'name': 'Time',
            'min': min_with_buffer,
            'axisLabel': {
                ':formatter': """
                    function (value) {
                    const min = Math.floor(value / 60);
                    const sec = (value % 60).toFixed(2).padStart(5, '0');
                    return min + ':' + sec;
                    }"""
            }
        },
        'series': series
    }
    session['chart'].options.clear()
    session['chart'].options.update(option)
    session['chart'].update()
    session['chart'].visible = True

async def update_results_table(course):
    session = app.storage.tab
    cols = ["Event", "SwimTime", "Age", "Points", "TimeStandard", "Meet", "Team", "SwimDate"]
    if course == "SCY":
        session['event_results_table'].columns = [{'name': col, 'label': col, 'field': col} for col in cols]
        session['event_results_table'].rows = session['scy_df'].to_dict('records')
    else:
        session['event_results_table'].columns = [{'name': col, 'label': col, 'field': col} for col in cols]
        session['event_results_table'].rows = session['lcm_df'].to_dict('records')
    session['event_results_table'].visible = True
    session['event_results_table'].update()

async def update_upcoming_meets_table():
    session = app.storage.tab
    session['upcoming_meets_table'].visible = True
    session['upcoming_meets_table'].update()

async def update_ncaa_comparison_table(event):
    session = app.storage.tab
    session['ncaa_comparisons'] = await fetch_ncaa_comp_data(str_to_timedelta(session['current_event_besttime'].strip('r')).total_seconds(), session['person']['Sex'], event)
    session['ncaa_comparisons'] = ["{:.2f}".format(r['pct_faster']) + "%" for r in session['ncaa_comparisons']]
    session['ncaa_comparison_table'].columns = [{'name': col, 'label': col, 'field': col} for col in ["BestTime", "Division I", "Division II", "Division III"]]
    session['ncaa_comparison_table'].rows = [{'BestTime': session['current_event_besttime'].strip('r'), 'Division I' : session['ncaa_comparisons'][0], 'Division II' : session['ncaa_comparisons'][1], 'Division III' : session['ncaa_comparisons'][2]}]
    session['ncaa_comparison_table'].visible = True
    session['ncaa_comparison_table'].update()

async def update_best_rankings_table():
    await ui.context.client.connected()
    session = app.storage.tab
    session['best_rankings_table'].rows = []
    session['best_rankings_table'].columns = [{'name': col, 'label': col, 'field': col} for col in ["Event", "SwimTime","Age", "Points", "TimeStandard", "Meet", "Team", "SwimDate"]]
    if not session['scy_df'].empty:
        scy_copy = session['scy_df'].copy()
        scy_copy['SwimTime'] = scy_copy['SwimTime'].apply(lambda x: str_to_datetime(x.replace('r', "")))
        scy_min_row = session['scy_df'].loc[scy_copy['SwimTime'].idxmin()].to_dict()  
        session['best_rankings_table'].rows.append(scy_min_row)
        session['current_event_besttime'] = scy_min_row['SwimTime']
    if not session['lcm_df'].empty:
        lcm_copy = session['lcm_df'].copy()
        lcm_copy['SwimTime'] = lcm_copy['SwimTime'].apply(lambda x: str_to_datetime(x.replace('r', "")))
        lcm_min_row = session['lcm_df'].loc[lcm_copy['SwimTime'].idxmin()].to_dict()
        session['best_rankings_table'].rows.append(lcm_min_row)
    session['best_rankings_table'].visible = True
    session['best_rankings_table'].update()

async def update_season_rankings_table():
    session = app.storage.tab
    session['season_rankings_table'].columns = [
        {'name': 'Event', 'label': 'Event', 'field': 'Event'},
        {'name': 'SwimDate', 'label': 'Swim Date', 'field': 'SwimDate'},
        {'name': 'AgeGroup', 'label': 'Age Group', 'field': 'AgeGroup'},
        {'name': 'LSC', 'label': 'LSC', 'field': 'LSC'},
        {'name': 'Team', 'label': 'Team', 'field': 'Team'},
        {'name': 'Meet', 'label': 'Meet', 'field': 'Meet'},
        {'name': 'SwimTime', 'label': 'Swim Time', 'field': 'SwimTime'},
        {'name': 'national_rank', 'label': 'National Rank', 'field': 'national_rank'},
        {'name': 'lsc_rank', 'label': 'LSC Rank', 'field': 'lsc_rank'},
        {'name': 'team_rank', 'label': 'Team Rank', 'field': 'team_rank'}
    ]

    session['season_rankings_table'].add_slot('body-cell-national_rank', """
        <q-td :props="props">
            <q-btn flat dense color="primary"
                :label="props.row['national_rank']"
                @click="() => $parent.$emit('open_rank_page', {type: 'National', row: props.row})"/>
        </q-td>
    """)

    session['season_rankings_table'].add_slot('body-cell-lsc_rank', """
        <q-td :props="props">
            <q-btn flat dense color="secondary"
                :label="props.row['lsc_rank']"
                @click="() => $parent.$emit('open_rank_page', {type: 'LSC', row: props.row})"/>
        </q-td>
    """)

    session['season_rankings_table'].add_slot('body-cell-team_rank', """
        <q-td :props="props">
            <q-btn flat dense color="accent"
                :label="props.row['team_rank']"
                @click="() => $parent.$emit('open_rank_page', {type: 'Team', row: props.row})"/>
        </q-td>
    """)


    def open_rank_page(msg):
        rank_type = msg.args['type']
        row = msg.args['row']
        ui.navigate.to(f"/rankings?rank_type={rank_type}&event={row['Event']}&age_group={row['AgeGroup']}&lsc={row['LSC']}&team={row['Team']}&sex={int(row['Sex'])}")
    session['season_rankings_table'].on('open_rank_page', open_rank_page)
    session['season_rankings_table'].rows = []
    start_str, end_str = [s.strip() for s in session['current_season'].split("-")]
    season_start = pd.to_datetime(start_str)
    season_end   = pd.to_datetime(end_str)
    if not session['scy_df'].empty:
        scy_copy = session['scy_df'].copy()
        scy_copy.drop(columns=['Points', 'TimeStandard'])
        scy_copy['SwimDate'] = scy_copy['SwimDate'].apply(lambda x: pd.to_datetime(x))
        scy_min_season_row = scy_copy[(scy_copy["SwimDate"] >= season_start) & (scy_copy["SwimDate"] <= season_end) & (scy_copy["national_rank"] > 0)].to_dict(orient='records')
        if scy_min_season_row:
            scy_min_season_row[0]["SwimDate"] = scy_min_season_row[0]["SwimDate"].strftime('%m/%d/%Y')
            session['season_rankings_table'].rows.extend(scy_min_season_row)
    if not session['lcm_df'].empty:
        lcm_copy = session['lcm_df'].copy()
        lcm_copy['SwimDate'] = lcm_copy['SwimDate'].apply(lambda x: pd.to_datetime(x))
        lcm_min_season_row = lcm_copy[(lcm_copy["SwimDate"] >= season_start) & (lcm_copy["SwimDate"] <= season_end) & (lcm_copy["national_rank"] > 0)].to_dict(orient='records')
        if lcm_min_season_row:
            lcm_min_season_row[0]["SwimDate"] = lcm_min_season_row[0]["SwimDate"].strftime('%m/%d/%Y')
            session['season_rankings_table'].rows.extend(lcm_min_season_row)

    session['season_rankings_table'].visible = True
    session['season_rankings_table'].update()

async def display_event_data(e, df):
    await ui.context.client.connected()
    session = app.storage.tab
    session['lcm_df'] = df.loc[df['Event'].str.contains("LCM")]
    session['scy_df'] = df.loc[df['Event'].str.contains("SCY")]
    
    await update_best_rankings_table()
    await update_ncaa_comparison_table(e)
    await update_upcoming_meets_table()
    await update_season_rankings_table()
    with session['results_column']:
        if session['event_label']:
            session['event_label'].set_text(e + " Progression")
            session['event_label'].update()
    await update_results_table(session['course_radio'].value)
    await update_progression_chart()

async def make_event_buttons(all_event_data_df):
    event_pairs = [('50 FR SCY', '50 FR LCM'), ('100 FR SCY', '100 FR LCM'),
              ('200 FR SCY', '200 FR LCM'), ('500 FR SCY', '400 FR LCM'),
              ('1000 FR SCY', '800 FR LCM'), ('1650 FR SCY', '1500 FR LCM'),
              ('50 FL SCY', '50 FL LCM'), ('100 FL SCY', '100 FL LCM'), ('200 FL SCY', '200 FL LCM'),
              ('50 BR SCY', '50 BR LCM'), ('100 BR SCY', '100 BR LCM'), ('200 BR SCY', '200 BR LCM'),
              ('50 BK SCY', '50 BK LCM'), ('100 BK SCY', '100 BK LCM'), ('200 BK SCY', '200 BK LCM'),
              ('100 IM SCY'), ('200 IM SCY', '200 IM LCM'), ('400 IM SCY', '400 IM LCM')
            ]
    
    first_non_empty_event = None
    first_non_empty_event_df = None
    first = True
    with ui.column().classes('w-full items-center'):
        with ui.row():
            for pair in event_pairs:
                event_df = all_event_data_df.loc[(all_event_data_df['Event'] == pair[0]) | (all_event_data_df['Event'] == pair[1])]
                if not event_df.empty:
                    if first:
                        first_non_empty_event = pair[0]
                        first_non_empty_event_df = event_df
                        first = False
                    ui.button(pair[0].split('SCY')[0], on_click=lambda e=pair[0], df=event_df: display_event_data(e, df))

    return (first_non_empty_event, first_non_empty_event_df)

@ui.page('/swimmer/{person_key}')
async def graph_page(person_key: str):
    await ui.context.client.connected()
    session = app.storage.tab
    session['keyboard'] = ui.keyboard(on_key=handle_key)
    name = session['person']['Name']
    age = session['person']['Age']
    lsc = session['person']['LSC']
    try:
        team = session['person']['Team']
    except:
        team = session['person']['Team']
    sex = session['person']['Sex']

    navbar() 

    with ui.column().classes('w-full items-center'):
        ui.label(name).style('font-size: 28px')
        with ui.grid(columns=2).classes('fit-content gap-0 bg-gray-300'):
            def cell(text, extra_classes=''):
                return ui.label(text).classes(
                    f'bg-white border border-gray-300 p-5 {extra_classes}'
                )
            cell('Team', 'p-1 border-b-0 border-r-0')
            cell(team,  'text-base font-medium tracking-wide text-primary cursor-pointer hover:bg-gray-100 border-b-0').on('click', lambda e, t=team: ui.navigate.to(f'/team/{t}'))
            cell('LSC', 'border-b-0 border-r-0')
            cell(lsc, 'border-b-0')
            cell('Current Age', 'border-b-0 border-r-0')
            cell(age, 'border-b-0')
            cell('Sex', 'border-r-0')
            cell("Male" if sex == 0 else "Female")

    all_event_data = await collect_all_event_data(person_key)
    session['all_event_data_df'] = pd.DataFrame(all_event_data, columns=["Event", "Sex", "SwimTime", "Relay", 
                                                            "Age", "AgeGroup", "Points", "TimeStandard", "LSC",
                                                            "Meet", "Team", "SwimDate", "national_rank", "lsc_rank", "team_rank"])
    session['all_event_data_df'].sort_values(by='SwimDate', inplace=True, ascending=False)
    session['all_event_data_df']["SwimTime"] = session['all_event_data_df'].apply(lambda row: convert_timedelta(row['SwimTime']) + "r" if row['Relay'] == 1 else convert_timedelta(row['SwimTime']), axis=1)
    session['all_event_data_df']["SwimDate"] = session['all_event_data_df']["SwimDate"].apply(lambda x: x.strftime('%m/%d/%Y'))
    session['all_event_data_df'].drop('Relay', axis=1, inplace=True)
    first_non_empty_event, first_non_empty_event_df = await make_event_buttons(session['all_event_data_df'])
    session['lcm_df'] = first_non_empty_event_df.loc[first_non_empty_event_df['Event'].str.contains("LCM")]
    session['scy_df'] = first_non_empty_event_df.loc[first_non_empty_event_df['Event'].str.contains("SCY")]

    session['best_times_column'] = ui.column().classes('w-full items-center')
    session['ncaa_comparison_column'] = ui.column().classes('w-full items-center')
    session['upcoming_meets_column'] = ui.column().classes('w-full items-center')
    session['season_rankings_column'] = ui.column().classes('w-full items-center')
    session['results_column'] = ui.column().classes('w-full items-center')
    with session['best_times_column']:
        session['best_times_label'] = ui.label('Best Times').style('font-size: 20px')
        session['best_rankings_table'] = ui.table(rows=[])
        session['best_rankings_table'].visible = False
    with session['ncaa_comparison_column']:
        session['ncaa_comparison_label'] = ui.label("""NCAA Comparison (Better than % of Swimmers)""").style('font-size: 20px')
        session['ncaa_comparison_table'] = ui.table(rows=[])
        session['ncaa_comparison_table'].visible = False
    with session['upcoming_meets_column']:
        session['meets_label'] = ui.label('Upcoming Championship Meets').style('font-size: 20px')
        session['upcoming_meets_table'] = ui.table(rows=[])
        session['upcoming_meets_table'].visible = False
    with session['season_rankings_column']: 
        session['season_rankings_label'] = ui.label('Current Season Rankings (' + session['current_season'] + ')').style('font-size: 20px')
        session['season_rankings_table'] = ui.table(rows=[])
        session['season_rankings_table'].visible = False
    with session['results_column']:
        session['event_label'] = ui.label(first_non_empty_event + " Progression").style('font-size: 20px')
        session['course_radio'] = ui.radio(["SCY", "LCM"], value="SCY", on_change=lambda: update_results_table(session['course_radio'].value)).props('inline')
        session['event_results_table'] = ui.table(rows=[])
        session['event_results_table'].visible = False
        session['chart'] = ui.echart({'series': []}).style('height: 600px; width: 100%; min-height: 600px;')
        session['chart'].visible = False

    await display_event_data(first_non_empty_event, first_non_empty_event_df)

@ui.page('/')
async def main_page():
    await ui.context.client.connected()
    session = app.storage.tab
    session['id_table_df'] = pd.DataFrame()
    session['id_table'] = None
    session['keyboard'] = ui.keyboard(on_key=handle_key)

    navbar() 
    session['main_page_column'] = ui.column().classes('w-full items-center')
    get_current_season()
    
    await get_global_pool()
    with session['main_page_column']:
        ui.label('SwimRank').style('font-size: 200%; font-weight: 300, font-family: "Times New Roman", Times, serif;')
        ui.label('This website provides swimming results and rankings data for competitive swimmers in the United States').style('font-size: 15px; margin-bottom: 20px;')
        ui.separator()
        ui.label('Swimmer Search').style('font-size: 200%; font-weight: 300, font-family: "Times New Roman", Times, serif;')
        ui.label('This website contains data for over 1 million swimmers over the past 10 years').style('font-size: 15px')
        session['search_input'] = ui.input(label='Enter name...', placeholder='Type a name...')
        session['search_input'].on('keypress.enter', lambda: fetch_people(session['search_input'].value))
        
        session['id_table'] = ui.table(rows=[], columns=[])
        session['id_table'].visible = False
        if not session['id_table_df'].empty:
            await update_id_table()

async def show_page():
    PAGE_SIZE = 50
    await ui.context.client.connected()
    session = app.storage.tab

    scy_start = (session['current_scy_page']['num'] - 1) * PAGE_SIZE
    scy_end   = scy_start + PAGE_SIZE

    lcm_start = (session['current_lcm_page']['num'] - 1) * PAGE_SIZE
    lcm_end   = lcm_start + PAGE_SIZE

    scy_page = session['current_scy_rank_selection'].iloc[scy_start:scy_end]
    lcm_page = session['current_lcm_rank_selection'].iloc[lcm_start:lcm_end]
    # Map rank column
    rank_map = {
        'National': 'national_rank',
        'LSC': 'lsc_rank',
        'Team': 'team_rank'
    }

    rank_col = rank_map[session['rank_type_select'].value]

    columns = [
        {'name': 'Name', 'label': 'Name', 'field': 'Name'},
        {'name': 'LSC', 'label': 'LSC', 'field': 'LSC'},
        {'name': 'Team', 'label': 'Team', 'field': 'Team'},
        {'name': 'SwimTime', 'label': 'Time', 'field': 'SwimTime'},
        {'name': rank_col, 'label': 'Rank', 'field': rank_col},
    ]

    session['ranking_table_scy'].columns = columns
    session['ranking_table_scy'].rows = scy_page.to_dict('records')
    session['ranking_table_scy'].visible = not scy_page.empty
    session['ranking_table_scy'].add_slot('body-cell-Name', """
        <q-td :props="props">
            <q-btn @click="() => $parent.$emit('person_selected', props.row)" 
                    :label="props.row.Name" 
                    flat dense color='primary'/>
        </q-td>
    """)

    session['ranking_table_lcm'].columns = columns
    session['ranking_table_lcm'].rows = lcm_page.to_dict('records')
    session['ranking_table_lcm'].visible = not lcm_page.empty
    session['ranking_table_lcm'].add_slot('body-cell-Name', """
        <q-td :props="props">
            <q-btn @click="() => $parent.$emit('person_selected', props.row)" 
                    :label="props.row.Name" 
                    flat dense color='primary'/>
        </q-td>
    """)

    def on_person_selected(msg):
        person = msg.args  # full row data (Name, Age, etc.)
        # store full info in session (not in URL)
        session['person'] = person
        # navigate using only the person key
        ui.navigate.to(f'/swimmer/{person["PersonKey"]}')

    session['ranking_table_scy'].on('person_selected', on_person_selected)
    session['ranking_table_lcm'].on('person_selected', on_person_selected)

    # Page buttons
    scy_total = math.ceil(len(session['current_scy_rank_selection']) / PAGE_SIZE)
    lcm_total = math.ceil(len(session['current_lcm_rank_selection']) / PAGE_SIZE)

    session['scy_prev'].visible = session['current_scy_page']['num'] > 1
    session['scy_next'].visible = session['current_scy_page']['num'] < scy_total

    session['lcm_prev'].visible = session['current_lcm_page']['num'] > 1
    session['lcm_next'].visible = session['current_lcm_page']['num'] < lcm_total

async def next_scy_page():
    await ui.context.client.connected()
    session = app.storage.tab
    session['current_scy_page']['num'] += 1
    await show_page()

async def prev_scy_page():
    await ui.context.client.connected()
    session = app.storage.tab
    if session['current_scy_page']['num'] > 1:
        session['current_scy_page']['num'] -= 1
        await show_page()

async def next_lcm_page():
    await ui.context.client.connected()
    session = app.storage.tab
    session['current_lcm_page']['num'] += 1
    await show_page()

async def prev_lcm_page():
    await ui.context.client.connected()
    session = app.storage.tab
    if session['current_lcm_page']['num'] > 1:
        session['current_lcm_page']['num'] -= 1
        await show_page()

async def refresh_table(event_map):
    await ui.context.client.connected()
    session = app.storage.tab
    
    sex = 0 if session['sex_select'].value == 'Male' else 1
    ev = session['event_select'].value
    ag = session['age_select'].value
    season = session['season_select'].value
    rt = session['rank_type_select'].value
    ls = session['lsc_select'].value
    cl = session['team_select'].value

    scy_table, lcm_table = event_map[ev + " SCY"]
    rows = await fetch_ranking_data(scy_table, lcm_table, ag, sex, season)
    temp = pd.DataFrame(rows, columns=["Event", "Name", "Sex", "PersonKey", "Age", "LSC", "Team", "SwimTime", "national_rank", "lsc_rank", "team_rank"])
    temp['SwimTime'] = temp.apply(lambda row: convert_timedelta(row['SwimTime']), axis=1)
    session['scy_ranking_data'] = temp[temp['Event'].str.contains("SCY")]
    session['lcm_ranking_data'] = temp[temp['Event'].str.contains("LCM")]
    
    if rt == 'LSC' and ls:
        session['current_scy_rank_selection'] = session['scy_ranking_data'][session['scy_ranking_data']['LSC'] == ls].drop(columns=['national_rank', 'team_rank'])
        session['current_lcm_rank_selection'] = session['lcm_ranking_data'][session['lcm_ranking_data']['LSC'] == ls].drop(columns=['national_rank', 'team_rank'])
    elif rt == 'Team' and cl:
        session['current_scy_rank_selection'] = session['scy_ranking_data'][session['scy_ranking_data']['Team'] == ls].drop(columns=['national_rank', 'lsc_rank'])
        session['current_lcm_rank_selection'] = session['lcm_ranking_data'][session['lcm_ranking_data']['Team'] == ls].drop(columns=['national_rank', 'lsc_rank'])
    else:
        session['current_scy_rank_selection'] = session['scy_ranking_data'].drop(columns=['lsc_rank', 'team_rank'])
        session['current_lcm_rank_selection'] = session['lcm_ranking_data'].drop(columns=['lsc_rank', 'team_rank'])
    
    session['current_scy_page']['num'] = 1
    session['current_lcm_page']['num'] = 1
    await show_page()

async def refresh_table_ranksys():
    await ui.context.client.connected()
    session = app.storage.tab
    rt = session['rank_type_select'].value
    ls = session['lsc_select'].value
    cl = session['team_select'].value
    if rt == 'LSC' and ls:
        session['current_scy_rank_selection'] = session['scy_ranking_data'][session['scy_ranking_data']['LSC'] == ls].drop(columns=['national_rank', 'team_rank'])
        session['current_lcm_rank_selection'] = session['lcm_ranking_data'][session['lcm_ranking_data']['LSC'] == ls].drop(columns=['national_rank', 'team_rank'])
    elif rt == 'Team' and cl:
        session['current_scy_rank_selection'] = session['scy_ranking_data'][session['scy_ranking_data']['Team'] == cl].drop(columns=['national_rank', 'lsc_rank'])
        session['current_lcm_rank_selection'] = session['lcm_ranking_data'][session['lcm_ranking_data']['Team'] == cl].drop(columns=['national_rank', 'lsc_rank'])
    else:
        session['current_scy_rank_selection'] = session['scy_ranking_data'].drop(columns=['lsc_rank', 'team_rank'])
        session['current_lcm_rank_selection'] = session['lcm_ranking_data'].drop(columns=['lsc_rank', 'team_rank'])
    
    session['current_scy_page']['num'] = 1
    session['current_lcm_page']['num'] = 1
    await show_page()

@ui.page('/rankings')
async def rankings_page(rank_type: str = 'National', event = '50 FR SCY', age_group = '13-14', lsc = '', team = '', sex: int = 0):
    await ui.context.client.connected()
    session = app.storage.tab
    navbar() 

    event_map = {
        '50 FR SCY'   : ("50_FR_SCY_results",   "50_FR_LCM_results"), '50 FR LCM'   : ("50_FR_SCY_results",   "50_FR_LCM_results"),
        '100 FR SCY'  : ("100_FR_SCY_results",  "100_FR_LCM_results"), '100 FR LCM'  : ("100_FR_SCY_results",  "100_FR_LCM_results"),
        '200 FR SCY'  : ("200_FR_SCY_results",  "200_FR_LCM_results"), '200 FR LCM'  : ("200_FR_SCY_results",  "200_FR_LCM_results"),
        '500 FR SCY'  : ("500_FR_SCY_results",  "400_FR_LCM_results"), '400 FR LCM'  : ("500_FR_SCY_results",  "400_FR_LCM_results"),
        '1000 FR SCY' : ("1000_FR_SCY_results", "800_FR_LCM_results"), '800 FR LCM'  : ("1000_FR_SCY_results", "800_FR_LCM_results"),
        '1650 FR SCY' : ("1650_FR_SCY_results", "1500_FR_LCM_results"), '1500 FR LCM' : ("1650_FR_SCY_results", "1500_FR_LCM_results"),
        '50 FL SCY'   : ("50_FL_SCY_results",   "50_FL_LCM_results"), '50 FL LCM'   : ("50_FL_SCY_results",   "50_FL_LCM_results"),
        '100 FL SCY'  : ("100_FL_SCY_results",  "100_FL_LCM_results"), '100 FL LCM'  : ("100_FL_SCY_results",  "100_FL_LCM_results"),
        '200 FL SCY'  : ("200_FL_SCY_results",  "200_FL_LCM_results"), '200 FL LCM'  : ("200_FL_SCY_results",  "200_FL_LCM_results"),
        '50 BK SCY'   : ("50_BK_SCY_results",   "50_BK_LCM_results"), '50 BK LCM'   : ("50_BK_SCY_results",   "50_BK_LCM_results"),
        '100 BK SCY'  : ("100_BK_SCY_results",  "100_BK_LCM_results"), '100 BK LCM'  : ("100_BK_SCY_results",  "100_BK_LCM_results"),
        '200 BK SCY'  : ("200_BK_SCY_results",  "200_BK_LCM_results"), '200 BK LCM'  : ("200_BK_SCY_results",  "200_BK_LCM_results"),
        '50 BR SCY'   : ("50_BR_SCY_results",   "50_BR_LCM_results"), '50 BR LCM'   : ("50_BR_SCY_results",   "50_BR_LCM_results"),
        '100 BR SCY'  : ("100_BR_SCY_results",  "100_BR_LCM_results"), '100 BR LCM'  : ("100_BR_SCY_results",  "100_BR_LCM_results"),
        '200 BR SCY'  : ("200_BR_SCY_results",  "200_BR_LCM_results"), '200 BR LCM'  : ("200_BR_SCY_results",  "200_BR_LCM_results"),
        '100 IM SCY'  : ("100_IM_SCY_results",  None),
        '200 IM SCY'  : ("200_IM_SCY_results",  "200_IM_LCM_results"), '200 IM LCM'  : ("200_IM_SCY_results",  "200_IM_LCM_results"),
        '400 IM SCY'  : ("400_IM_SCY_results",  "400_IM_LCM_results"), '400 IM LCM'  : ("400_IM_SCY_results",  "400_IM_LCM_results"),}

    scy_table, lcm_table = event_map[event]
    season = get_current_season()
    if age_group == "10 ":
        age_group = '10 & Under'
    event = event.split('SCY')[0].split('LCM')[0].strip()
    rows = await fetch_ranking_data(scy_table, lcm_table, age_group, sex, season)
    temp = pd.DataFrame(rows, columns=["Event", "Name", "Sex", "PersonKey", "Age", "LSC", "Team", "SwimTime", "national_rank", "lsc_rank", "team_rank"])
    temp['SwimTime'] = temp.apply(lambda row: convert_timedelta(row['SwimTime']), axis=1)
    session['scy_ranking_data'] = temp[temp['Event'].str.contains("SCY")]
    session['lcm_ranking_data'] = temp[temp['Event'].str.contains("LCM")]
    all_events =['50 FR', '100 FR', '200 FR', '500 FR', '1000 FR', '1650 FR', '50 FL', '100 FL', '200 FL',
                 '50 BK', '100 BK', '200 BK', '50 BR', '100 BR', '200 BR', '100 IM', '200 IM', '400 IM']
    all_sex= ['Male', 'Female']
    all_age_groups = ['10 & Under', '11-12', '13-14', '15-16', '17-18', '19 & Over']
    all_lscs = sorted(temp['LSC'].dropna().unique().tolist())
    all_teams = sorted(temp['Team'].dropna().unique().tolist())

    start_str, end_str = season.split(" - ")
    start_month_day, start_year = start_str.rsplit("/", 1)
    end_month_day, end_year = end_str.rsplit("/", 1)
    start_year = int(start_year)
    end_year = int(end_year)
    all_seasons = [f"{start_month_day}/{start_year - i} - {end_month_day}/{end_year - i}" for i in range(10)]

    # Reactive variables
    session['current_scy_page'] = {'num': 1}
    session['current_lcm_page'] = {'num': 1}
    # Dropdowns row
    with ui.row().classes('w-full no-wrap items-start gap-4'):
        # ---------------- FILTERS COLUMN ----------------
        with ui.column().classes('w-64 p-4 gap-4 bg-gray-50 rounded shadow-sm items-center'):
            ui.label("Filters").classes('text-lg font-bold')
            session['season_select'] = ui.select(
                options=all_seasons,
                value=season,
                label='Season',
                on_change=lambda: refresh_table(event_map)
            ).classes('w-full')

            session['rank_type_select'] = ui.select(
                options=['National', 'LSC', 'Team'],
                value=rank_type,
                label='Rank Type',
                on_change=lambda: refresh_table_ranksys()
            ).classes('w-full')

            session['sex_select'] = ui.select(
                options=all_sex,
                value='Male' if sex == 0 else 'Female',
                label='Sex',
                on_change=lambda: refresh_table(event_map)
            ).classes('w-full')

            session['event_select'] = ui.select(
                options=all_events,
                value=event if event in all_events else all_events[0],
                label='Event',
                on_change=lambda: refresh_table(event_map)
            ).classes('w-full')

            session['age_select'] = ui.select(
                options=all_age_groups,
                value=age_group if age_group in all_age_groups else all_age_groups[0],
                label='Age Group',
                on_change=lambda: refresh_table(event_map)
            ).classes('w-full')

            session['lsc_select'] = ui.select(
                options=all_lscs,
                value=lsc if lsc in all_lscs else None,
                label='LSC',
                on_change=lambda: refresh_table_ranksys()
            ).bind_visibility_from(
                session['rank_type_select'], 'value',
                backward=lambda v: v == 'LSC'
            ).classes('w-full')

            session['team_select'] = ui.select(
                options=all_teams,
                value=team if team in all_teams else None,
                label='Team',
                with_input=True,
                on_change=lambda: refresh_table_ranksys()
            ).props(
                'dense outlined clearable'
            ).bind_visibility_from(
                session['rank_type_select'], 'value',
                backward=lambda v: v == 'Team'
            ).classes('w-full')

        # ---------------- SCY TABLE ----------------
        with ui.column().classes('flex-1 gap-2 justify-center items-center'):
            ui.label("SCY Results").classes('text-lg font-semibold')
            session['ranking_table_scy'] = ui.table(
                rows=[],
                columns=[]
            ).classes('w-full max-h-[600px] overflow-y-auto')

            with ui.row().classes('justify-between w-full mt-2 items-center'):
                session['scy_prev'] = ui.button('Prev SCY', on_click=prev_scy_page)
                ui.label().bind_text_from(
                    session['current_scy_page'],
                    'num',
                    backward=lambda v: f"Page {v}"
                )

                session['scy_next'] = ui.button(
                    'Next SCY',
                    on_click=lambda: next_scy_page()
                )

        # ---------------- LCM TABLE ----------------
        with ui.column().classes('flex-1 gap-2 items-center'):
            ui.label("LCM Results").classes('text-lg font-semibold')
            session['ranking_table_lcm'] = ui.table(
                rows=[],
                columns=[]
            ).classes('w-full max-h-[600px] overflow-y-auto')

            with ui.row().classes('justify-between w-full mt-2 items-center'):
                session['lcm_prev'] = ui.button('Prev LCM', on_click=prev_lcm_page)

                ui.label().bind_text_from(
                    session['current_lcm_page'],
                    'num',
                    backward=lambda v: f"Page {v}"
                )

                session['lcm_next'] = ui.button(
                    'Next LCM',
                    on_click=lambda: next_lcm_page()
                )
    
    await refresh_table_ranksys()
    await show_page()

@ui.page('/discussion')
async def discussion_page():
    navbar()

    with ui.column().classes('w-full items-center py-10 px-6 gap-4'):
        ui.label('Discussion Forum').classes('text-3xl font-bold')

        ui.label('Coming soon 🚧').classes('text-gray-600 text-lg')

        # Placeholder box
        ui.input(placeholder='Start a new topic...').classes('w-1/2')
        ui.button('Post').classes('mt-2')

@ui.page('/team/{team}')
async def team_page(team: str):
    await ui.context.client.connected()
    session = app.storage.tab
    navbar()

    session['team_df'] = await fetch_team_swimmers(team)

    session['team_df']['Name'] = (
        session['team_df']['FirstName'] + ' ' +
        session['team_df']['MiddleName'].fillna('') + ' ' +
        session['team_df']['LastName']
    ).str.replace('  ', ' ').str.strip()

    age_groups = {
        'All': None,
        '8 & Under': (0, 8),
        '9-10': (9, 10),
        '11-12': (11, 12),
        '13-14': (13, 14),
        '15-18': (15, 18),
        '18+': (18, 200),
    }
    def filter_df():
        group = session['team_age_select'].value
        if age_groups[group] is None:
            return session['team_df']
        low, high = age_groups[group]
        return session['team_df'][(session['team_df']['Age'] >= low) & (session['team_df']['Age'] <= high)]

    def update_tables():
        filtered = filter_df()
        males = filtered[filtered['Sex'] == 0]
        females = filtered[filtered['Sex'] == 1]

        session['team_male_table'].rows = males.to_dict('records')
        session['team_female_table'].rows = females.to_dict('records')

        session['team_male_table'].update()
        session['team_female_table'].update()
    
    def on_person_selected(msg):
        person = msg.args  # full row data (Name, Age, etc.)
        # store full info in session (not in URL)
        session['person'] = person
        # navigate using only the person key
        ui.navigate.to(f'/swimmer/{person["PersonKey"]}')
        
    with ui.row().classes('w-full justify-center'):
        ui.label(f'Team: {team}').classes('text-2xl font-bold mb-4')
    with ui.row().classes('w-full items-start no-wrap gap-3'):
        with ui.column().classes('w-64 p-4 gap-4 bg-gray-50 rounded shadow-sm items-center'):
            ui.label('Age Group').classes('text-lg font-semibold')
            session['team_age_select'] = ui.select(
                options=list(age_groups.keys()),
                value='All',
                on_change=lambda _: update_tables(),
            ).classes('w-full') 

        with ui.row().classes('flex-1 justify-center items-start gap-2'):
            with ui.column().classes('w-[420px] items-center'):
                ui.label('Male').classes('text-lg font-semibold mb-1')
                session['team_male_table'] = ui.table(
                    columns=[
                        {'name': 'Name', 'label': 'Name', 'field': 'Name'},
                        {'name': 'Age', 'label': 'Age', 'field': 'Age'},
                    ],
                    rows=[],
                ).classes('fit-content')
            
            with ui.column().classes('w-[420px] items-center'):
                ui.label('Female').classes('text-lg font-semibold mb-1')
                session['team_female_table'] = ui.table(
                    columns=[
                        {'name': 'Name', 'label': 'Name', 'field': 'Name'},
                        {'name': 'Age', 'label': 'Age', 'field': 'Age'},
                    ],
                    rows=[], 
                ).classes('fit-content')
    update_tables()
    session['team_female_table'].add_slot('body-cell-Name', """
        <q-td :props="props">
            <q-btn @click="() => $parent.$emit('person_selected', props.row)" 
                    :label="props.row.Name" 
                    flat dense color='primary'/>
        </q-td>
    """)
    session['team_female_table'].on('person_selected', on_person_selected)

    session['team_male_table'].add_slot('body-cell-Name', """
        <q-td :props="props">
            <q-btn @click="() => $parent.$emit('person_selected', props.row)" 
                    :label="props.row.Name" 
                    flat dense color='primary'/>
        </q-td>
    """)
    session['team_male_table'].on('person_selected', on_person_selected)

@ui.page('/aboutme')
async def aboutme_page():
    await ui.context.client.connected()
    session = app.storage.tab
    navbar()
    with ui.row().classes('w-full justify-center'):
        with ui.column().classes('w-3/5 items-center text-center'):
            ui.label('About Me').style('font-size: 28px')

        with ui.column().classes('w-3/5'):
            ui.label("Hello! My name is DW.").style('font-size: 15px')

            ui.label(
                "I was a competitive swimmer for over 12 years from 7 years old to now 21. "
                "During my swimming career, I swam for my high school team, club team, and college club team. "
                "Growing up, I used the swimmingrank.com website frequently to check my rankings and see how I compared "
                "to other swimmers in my age group and events. However, with that website no longer available, "
                "I decided to create SwimRank to fill that gap and provide swimmers with a similar resource to track "
                "their rankings and progress."
            ).style('font-size: 15px')

            with ui.row().classes('items-center no-wrap'):
                ui.label('Please contact me at').style('font-size: 15px')
                ui.link(
                    'DWwork178@gmail.com',
                    'mailto:DWwork178@gmail.com'
                ).classes('text-blue-600 hover:underline').style('font-size: 15px')

            ui.label("Finally, it does cost money to host the database and website, "
                "so if you would like to support the site please consider donating via the Donate page. Thank you!").style('font-size: 15px')

@ui.page('/privacy')
async def privacypolicy_page():
    await ui.context.client.connected()
    session = app.storage.tab
    navbar()
    with ui.row().classes('w-full justify-center'):
        with ui.column().classes('w-3/5 items-center text-center'):
            ui.label('Privacy Policy').style('font-size: 28px')
        with ui.column().classes('w-3/5'):
            ui.label("""I don't like ads or trackers either, so SwimRank is designed to be as privacy-friendly as possible. I do no track you activity on the site, nor do I use any third-party trackers or ads.
                        All of the data available on this website is publicly available from USA Swimming's website and is used here simply to compile and display that information in a more user-friendly manner.
                        I update this website weekly with the previous weeks meet results. Only meets registered with USA Swimming will be included in the rankings and results, so regular high school duel meets
                        or college meets may not be included.""").style('font-size: 15px')
        with ui.column().classes('w-3/5 items-center text-center'):
            ui.label('FAQ').style('font-size: 28px')
        with ui.column().classes('w-3/5'):
            ui.label("""1. How often is the data updated?""").style('font-size: 15px').classes('font-semibold')
            ui.label("""The data is updated weekly, typically on Mondays, to include the previous week's meet results.""").style('font-size: 15px')
            ui.label("""2. Where does the data come from?""").style('font-size: 15px').classes('font-semibold')
            ui.label("""All data is sourced from publicly available information on USA Swimming's website.""").style('font-size: 15px')
            ui.label("""3. Why are some meets or times missing?""").style('font-size: 15px').classes('font-semibold')
            ui.label("""Only meets that are officially registered with USA Swimming are included in the rankings and results. Regular high school duel meets or college meets may not be included. Additionally I have
                        only collected data up to 2016 so results from before that year will not be displayed.""").style('font-size: 15px')

def make_qr(data: str):
        qr = qrcode.make(data)
        buf = BytesIO()
        qr.save(buf, format='PNG')
        buf.seek(0)
        return buf

@ui.page('/donate')
def donate_page():
    navbar()
    ZELLE_EMAIL = 'alphadjw@gmail.com'

    with ui.row().classes('w-full justify-center'):
        with ui.column().classes('w-3/5 items-center text-center'):
            ui.label('Support This Website').style('font-size: 28px')

            ui.label('Donate securely using Zelle through your bank app.').classes('text-center text-lg font-semibold')
            ui.image('static/zelle_qr.png').classes('w-48 h-48')

if __name__ in {"__main__", "__mp_main__"}:
    ui.run(title='SwimRank')
