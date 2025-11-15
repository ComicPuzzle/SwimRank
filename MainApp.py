import math
import time
from nicegui import ui, app
import pandas as pd
import asyncio
import asyncpg
from nicegui.events import KeyEventArguments
from datetime import datetime, timedelta
from get_credentials import get_credentials

# --- GLOBAL DB POOL ---
global_pool = None
pd.set_option('display.max_columns', None)
async def get_global_pool(dbname, ip, port, password):
    """Return a shared asyncpg pool for all requests."""
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
        session['current_season'] = f"{'9/' + str(session['current_year']) + ' - 8/' + str(session['current_year'] + 1)}"
    else:
        session['current_season'] = f"{'9/' + str(session['current_year'] - 1) + ' - 8/' + str(session['current_year'])}"\
        
@app.on_shutdown
async def shutdown():
    """Close global pool on app shutdown."""
    global global_pool
    if global_pool and not global_pool.is_closing():
        await global_pool.close()

def reset_session_vars():
    session = app.storage.tab
    session['dbname'], session['port'], session['password'], session['ip'] = get_credentials()
    session['course_radio'], session['best_times_label'], session['meets_label'], session['season_rankings_label'], session['career_rankings_label'] = None, None, None, None, None
    session['progress_label'], session['comparison_label'], session['results_column'], session['best_times_column'] = None, None, None, None
    session['upcoming_meets_column'], session['season_rankings_column'], session['ncaa_comparison_column'], session['best_rankings_table'] = None, None, None, None
    session['best_times_label'], session['ncaa_comparison_label'], session['chart'], session['event_label'] = None, None, None, None
    session['person'] = None
    
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
    # Handles times like "1:05.23" or "59.99"
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
        print("Ctrl pressed")
        session['control_timer'] = time.time()
    elif e.key == 'c' and e.action.keyup:
        print("c pressed")
        if time.time() - session.get('control_timer', 0) < 0.5:
            app.shutdown()  # Stop the NiceGUI application

async def fetch_people(name):
    name = name.lower().strip().split()
    split_name = ''.join('%' + part for part in name) + '%'
    session = app.storage.tab

    query = f"""SELECT * FROM "ResultsSchema"."SwimmerIDs" WHERE LOWER("Name") LIKE '{split_name}'"""
    pool = await get_global_pool(session['dbname'], session['ip'], session['port'], session['password'])
    async with pool.acquire() as con:
        rows = await con.fetch(query)
    session['id_table_df'] = pd.DataFrame(rows, columns=['Name', 'Age', 'LSC', 'Club', 'PersonKey', 'Sex'])
    await update_id_table()


async def fetch_person_event_data(table, key, dbname, ip, port, password):
    query = f"""SELECT "event", "swimtime", "relay", "age", "points", "timestandard",  
                            "meet", "team", "swimdate" FROM "ResultsSchema"."{table}" WHERE "personkey" = {key}"""
    pool = await get_global_pool(dbname, ip, port, password)
    async with pool.acquire() as con:
        rows = await con.fetch(query)
    return rows

async def fetch_person_season_rank(table, dbname, ip, port, password, sex, age):
    if datetime.now().month >= 9:
        season_start_year = datetime.now().year
    else:
        season_start_year = datetime.now().year - 1 
    a = get_age_group(age)
    query = f"""SELECT 
                    ANY_VALUE(event) AS event, 
                    personkey,
                    sex, 
                    age, 
                    ANY_VALUE(team) AS team,
                    ANY_VALUE(lsc) AS lsc,
                    ANY_VALUE(usasswimtimekey) AS usasswimtimekey,
                    ANY_VALUE(meet) AS meet, 
	                ANY_VALUE(swimdate) AS swimdate,
                    ANY_VALUE(relay) AS relay,
                    MIN(swimtime) AS swimtime,
                RANK() OVER (ORDER BY MIN(swimtime)) AS rank
                FROM
                    "ResultsSchema"."{table}"
                WHERE
                    sex = {sex}
                    AND age BETWEEN {a[0]} AND {a[1]}
                    AND swimdate >= DATE '{season_start_year-1}-09-01'
                    AND swimdate <  DATE '{season_start_year}-09-01'
                GROUP BY personkey, sex, age
                ORDER BY swimtime
                LIMIT 1000"""
    pool = await get_global_pool(dbname, ip, port, password)
    async with pool.acquire() as con:
        rows = await con.fetch(query)
    return rows

async def collect_all_event_data(person_key):
    session = app.storage.tab
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
        tasks.append(fetch_person_event_data(table, person_key, session['dbname'],  session['ip'], session['port'], session['password']))
    results = await asyncio.gather(*tasks)
    all_event_data = [item for sublist in results if sublist for item in sublist]

    return all_event_data

async def collect_all_ranking_data(sex=0, age=18):
    await ui.context.client.connected()
    session = app.storage.tab
    db_table_names = ['50_FR_SCY_results', '50_FR_LCM_results', '100_FR_SCY_results', '100_FR_LCM_results',
                        '200_FR_SCY_results', '200_FR_LCM_results', '400_FR_LCM_results', '500_FR_SCY_results', 
                        '800_FR_LCM_results', '1000_FR_SCY_results', '1500_FR_LCM_results', '1650_FR_SCY_results',
                        '50_BK_SCY_results', '100_BK_SCY_results', '200_BK_SCY_results', '50_BK_LCM_results', '100_BK_LCM_results', '200_BK_LCM_results',
                        '50_FL_SCY_results', '100_FL_SCY_results', '200_FL_SCY_results', '50_FL_LCM_results', '100_FL_LCM_results', '200_FL_LCM_results',
                        '50_BR_SCY_results', '100_BR_SCY_results', '200_BR_SCY_results', '50_BR_LCM_results', '100_BR_LCM_results', '200_BR_LCM_results',
                        '100_IM_SCY_results', '200_IM_SCY_results', '400_IM_SCY_results', '200_IM_LCM_results', '400_IM_LCM_results'
                        ]
    tasks = []
    for s in [0, 1]:
        for a in [10, 12, 14, 16, 18]:
            for table in db_table_names:
                tasks.append(fetch_person_season_rank(table, session['dbname'],  session['ip'], session['port'], session['password'], s, a))
    results = await asyncio.gather(*tasks)
    season_ranking_data = [item for sublist in results if sublist for item in sublist]
    return season_ranking_data

async def update_id_table():
    await ui.context.client.connected()
    session = app.storage.tab
    session['id_table'].columns = [{'name': col, 'label': col, 'field': col} for col in session['id_table_df'].columns]
    session['id_table'].rows = session['id_table_df'].to_dict('records')

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
        person = msg.args  # full row data (Name, Age, etc.)
        # store full info in session (not in URL)
        session['person'] = person
        # navigate using only the person key
        ui.navigate.to(f'/swimmer/{person["PersonKey"]}')

    session['id_table'].on('person_selected', on_person_selected)

async def make_event_buttons(all_event_data_df):
    events = ['50 FR', '100 FR', '200 FR', '400/500 FR', 
              '800/1000 FR', '1500/1650 FR', '50 FL', 
              '100 FL', '200 FL', '50 BK', '100 BK', 
              '200 BK', '50 BR', '100 BR', '200 BR',
              '100 IM', '200 IM', '400 IM']
    
    first_non_empty_event = None
    first_non_empty_event_df = None
    first = True
    with ui.column().classes('w-full items-center'):
        with ui.row():
            for event in events:
                event_df = all_event_data_df.loc[all_event_data_df['event'].str.contains(event)]
                if not event_df.empty:
                    if first:
                        first_non_empty_event = event
                        first_non_empty_event_df = event_df
                        first = False
                    ui.button(event, on_click=lambda e=event, df=event_df: display_event_data(e, df))

    return (first_non_empty_event, first_non_empty_event_df)

async def update_results_table(course):
    await ui.context.client.connected()
    session = app.storage.tab
    if course == "SCY":
        session['event_results_table'].columns = [{'name': col, 'label': col, 'field': col} for col in session['scy_df'].columns]
        session['event_results_table'].rows = session['scy_df'].to_dict('records')
    else:
        session['event_results_table'].columns = [{'name': col, 'label': col, 'field': col} for col in session['lcm_df'].columns]
        session['event_results_table'].rows = session['lcm_df'].to_dict('records')
    session['event_results_table'].visible = True
    session['event_results_table'].update()
    #add graph

async def update_upcoming_meets_table():
    session = app.storage.tab
    session['upcoming_meets_table'].visible = True
    session['upcoming_meets_table'].update()

async def update_ncaa_comparison_table():
    session = app.storage.tab
    session['ncaa_comparison_table'].visible = True
    session['ncaa_comparison_table'].update()

async def update_season_rankings_table(e):
    session = app.storage.tab
    #session['season_rankings_table'].columns = [{'name': col, 'label': col, 'field': col} for col in ["event", "age group", "team", "meet", "swimdate", "swimtime", "national rank", "lsc rank", "club rank"]]
    session['season_rankings_table'].columns = [
        {'name': 'event', 'label': 'Event', 'field': 'event'},
        {'name': 'age group', 'label': 'Age Group', 'field': 'age group'},
        {'name': 'team', 'label': 'Team', 'field': 'team'},
        {'name': 'meet', 'label': 'Meet', 'field': 'meet'},
        {'name': 'swimdate', 'label': 'Swim Date', 'field': 'swimdate'},
        {'name': 'swimtime', 'label': 'Swim Time', 'field': 'swimtime'},
        {'name': 'national rank', 'label': 'National Rank', 'field': 'national rank'},
        {'name': 'lsc rank', 'label': 'LSC Rank', 'field': 'lsc rank'},
        {'name': 'club rank', 'label': 'Club Rank', 'field': 'club rank'}
    ]

    session['season_rankings_table'].add_slot('body-cell-national rank', """
        <q-td :props="props">
            <q-btn flat dense color="primary"
                :label="props.row['national rank']"
                @click="() => $parent.$emit('open_rank_page', {type: 'national', row: props.row})"/>
        </q-td>
    """)

    session['season_rankings_table'].add_slot('body-cell-lsc rank', """
        <q-td :props="props">
            <q-btn flat dense color="secondary"
                :label="props.row['lsc rank']"
                @click="() => $parent.$emit('open_rank_page', {type: 'lsc', row: props.row})"/>
        </q-td>
    """)

    session['season_rankings_table'].add_slot('body-cell-club rank', """
        <q-td :props="props">
            <q-btn flat dense color="accent"
                :label="props.row['club rank']"
                @click="() => $parent.$emit('open_rank_page', {type: 'club', row: props.row})"/>
        </q-td>
    """)

    def open_rank_page(msg):
        rank_type = msg.args['type']
        row = msg.args['row']
        ui.navigate.to(f"/rankings?rank_type={rank_type}&event={row['event']}&age_group={row['age group']}&lsc={row['lsc']}&club={row['team']}&sex={int(row['sex'])}")
    session['season_rankings_table'].on('open_rank_page', open_rank_page)

    rows = session['national_rank_data_df'].loc[(session['national_rank_data_df']['event'].astype(str).str.contains(str(e), case=False, na=False)) & (session['national_rank_data_df']['personkey'] == int(session['person']['PersonKey']))]
    rows = rows.to_dict('records')
    session['season_rankings_table'].rows.extend(rows)
    session['season_rankings_table'].visible = True
    session['season_rankings_table'].update()
    return

async def update_best_rankings_table():
    await ui.context.client.connected()
    session = app.storage.tab
    session['best_rankings_table'].columns = [{'name': col, 'label': col, 'field': col} for col in ["event", "swimtime","age", "points", "timestandard", "meet", "team", "swimdate"]]
    if not session['scy_df'].empty:
        scy_copy = session['scy_df'].copy()
        scy_copy['swimtime'] = scy_copy['swimtime'].apply(lambda x: str_to_datetime(x.replace('r', "")))
        scy_min_row = session['scy_df'].loc[scy_copy['swimtime'].idxmin()].to_dict()  
        session['best_rankings_table'].rows.append(scy_min_row)
    if not session['lcm_df'].empty:
        lcm_copy = session['lcm_df'].copy()
        lcm_copy['swimtime'] = lcm_copy['swimtime'].apply(lambda x: str_to_datetime(x.replace('r', "")))
        lcm_min_row = session['lcm_df'].loc[lcm_copy['swimtime'].idxmin()].to_dict()
        session['best_rankings_table'].rows.append(lcm_min_row)
    session['best_rankings_table'].visible = True
    session['best_rankings_table'].update()

async def update_progression_chart():
    # 1) prepare SCY series
    await ui.context.client.connected()
    session = app.storage.tab
    def get_series(df):
        df_copy = df.copy()
        df_copy['parsed_time'] = df_copy['swimtime'].apply(lambda x: str_to_timedelta(x.replace("r", "")))
        df_copy['parsed_date'] = pd.to_datetime(df_copy['swimdate'], format='%m/%d/%Y')
        df_copy.sort_values('parsed_date', inplace=True)
        return df_copy.apply(
            lambda row: [row['parsed_date'].strftime('%Y-%m-%d'),
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
            ':formatter': """
                function(params) {
                    function formatTime(seconds) {
                        const min = Math.floor(seconds / 60);
                        const sec = (seconds % 60).toFixed(2).padStart(5, '0');
                        return min > 0 ? `${min}:${sec}` : sec;
                    }
                    return params.map(p => 
                        p.seriesName + ' (' + p.value[0].slice(0, 10) + '): ' + formatTime(p.value[1])
                    ).join('<br/>');
                }
            """
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
    if session['chart']:
        session['chart'].delete()
    session['chart'] = ui.echart(options=option).style('height: 600px; width: 100%; min-height: 600px;')
    session['chart'].visible = True

async def display_event_data(e, df):
    await ui.context.client.connected()
    session = app.storage.tab
    session['lcm_df'] = df.loc[df['event'].str.contains("LCM")]
    session['scy_df'] = df.loc[df['event'].str.contains("SCY")]

    if not session['results_column']:
        session['best_times_column'] = ui.column().classes('w-full items-center')
        session['ncaa_comparison_column'] = ui.column().classes('w-full items-center')
        session['upcoming_meets_column'] = ui.column().classes('w-full items-center')
        session['season_rankings_column'] = ui.column().classes('w-full items-center')
        session['results_column'] = ui.column().classes('w-full items-center')
    
    with session['best_times_column']:
        if not session['best_times_label']:
            session['best_times_label'] = ui.label('Best Times').style('font-size: 28px')
        try:
            session['best_rankings_table'].delete()
        except:
            pass
        session['best_rankings_table'] = ui.table(rows=[])
        session['best_rankings_table'].visible = False
        await update_best_rankings_table()
    with session['ncaa_comparison_column']:
        if not session['ncaa_comparison_label']:
            session['ncaa_comparison_label'] = ui.label('NCAA Comparison')
        try:
            session['ncaa_comparison_table'].delete()
        except:
            pass
        session['ncaa_comparison_table'] = ui.table(rows=[])
        session['ncaa_comparison_table'].visible = False
        await update_ncaa_comparison_table()
    with session['upcoming_meets_column']:
        if not session['meets_label']:
            session['meets_label'] = ui.label('Upcoming Championship Meets')
        try:
            session['upcoming_meets_table'].delete()
        except:
            pass
        session['upcoming_meets_table'] = ui.table(rows=[])
        session['upcoming_meets_table'].visible = False
        await update_upcoming_meets_table()
    with session['season_rankings_column']: 
        if not session['season_rankings_label']:
            session['season_rankings_label'] = ui.label('Current Season Rankings (' + session['current_season'] + ')')
        try:
            session['season_rankings_table'].delete()
        except:
            pass
        session['season_rankings_table'] = ui.table(rows=[])
        session['season_rankings_table'].visible = False
        await update_season_rankings_table(e)
    with session['results_column']:
        if not session['event_label']: 
            session['event_label'] = ui.label(e + " Progression") 
        if not session['course_radio']:
            session['course_radio'] = ui.radio(["SCY", "LCM"], value="SCY", on_change=lambda: update_results_table(session['course_radio'].value)).props('inline')
        try:
            session['event_results_table'].delete()
        except:
            pass
        session['event_results_table'] = ui.table(rows=[])
        session['event_results_table'].visible = False
        await update_results_table(session['course_radio'].value)
        try:
            session['chart'].delete()
        except:
            pass
        session['chart'] = ui.echart([])
        session['chart'].visible = False
        await update_progression_chart()
        

@ui.page('/swimmer/{person_key}')
async def graph_page(person_key: str):
    await ui.context.client.connected()
    session = app.storage.tab
    session['keyboard'] = ui.keyboard(on_key=handle_key)
    name = session['person']['Name']
    age = session['person']['Age']
    lsc = session['person']['LSC']
    club = session['person']['Club']
    sex = session['person']['Sex']
    with ui.column().classes('w-full items-center'):
        with ui.row():
            ui.label(name).style('font-size: 200%; font-weight: 300; font-family: "Times New Roman", Times, serif;')
        with ui.row():
            with ui.grid(columns=2).classes('w-full gap-0'):
                ui.label('Team').classes('border p-3')
                ui.button(text=club, on_click=lambda: ui.notify(club))
                ui.label('LSC').classes('border p-3')
                ui.label(lsc)
                ui.label('Current Age').classes('border p-3')
                ui.label(age)
                ui.label('Sex').classes('border p-3')
                ui.label(sex).classes('border p-3')
    
    if session['previous_personkey'] != person_key:
        session['previous_personkey'] = person_key
        all_event_data = await collect_all_event_data(person_key)
        session['all_event_data_df'] = pd.DataFrame(all_event_data, columns=["event", "swimtime", "relay", 
                                                                "age", "points", "timestandard",  
                                                                "meet", "team", "swimdate"])
        session['all_event_data_df'].sort_values(by='swimdate', inplace=True, ascending=False)
        session['all_event_data_df']["swimtime"] = session['all_event_data_df'].apply(lambda row: convert_timedelta(row['swimtime']) + "r" if row['relay'] == 1 else convert_timedelta(row['swimtime']), axis=1)
        session['all_event_data_df']["swimdate"] = session['all_event_data_df']["swimdate"].apply(lambda x: x.strftime('%m/%d/%Y'))
        session['all_event_data_df'].drop('relay', axis=1, inplace=True)

        first_non_empty_event, first_non_empty_event_df = await make_event_buttons(session['all_event_data_df'])
        session['lcm_df'] = first_non_empty_event_df.loc[first_non_empty_event_df['event'].str.contains("LCM")]
        session['scy_df'] = first_non_empty_event_df.loc[first_non_empty_event_df['event'].str.contains("SCY")]
        await display_event_data(first_non_empty_event, first_non_empty_event_df)
    else:
        event = session['scy_df']['event'].iloc[0].split('SCY')[0].strip()
        e = session['all_event_data_df'][session['all_event_data_df']['event'].str.contains(event)]
        #session['lcm_df'] = e.loc[e['event'].str.contains("LCM")]
        #session['scy_df'] = e.loc[e['event'].str.contains("SCY")]
        await make_event_buttons(session['all_event_data_df'])
        session['results_column'].delete()
        session['best_times_column'].delete() 
        session['upcoming_meets_column'].delete()
        session['season_rankings_column'].delete()
        session['ncaa_comparison_column'].delete()
        reset_session_vars()
        await display_event_data(event, e)

@ui.page('/')
async def main_page():
    await ui.context.client.connected()
    session = app.storage.tab
    session['id_table_df'] = pd.DataFrame()
    session['id_table'] = None
    session['previous_personkey'] = None
    session['keyboard'] = ui.keyboard(on_key=handle_key)
    session['main_page_column'] = ui.column().classes('w-full items-center')
    get_current_season()
    reset_session_vars()
    
    await get_global_pool(session['dbname'],  session['ip'], session['port'], session['password'])
    """season_rank_data = await collect_all_ranking_data()
    session['national_rank_data_df'] = pd.DataFrame(season_rank_data, columns=["event", "personkey", "sex", "age", "team", "lsc", "usasswimtimekey", "meet", "swimdate", "relay", "swimtime", "rank"])
    session['national_rank_data_df']["swimtime"] = session['national_rank_data_df'].apply(lambda row: convert_timedelta(row['swimtime']) + "r" if row['relay'] == 1 else convert_timedelta(row['swimtime']), axis=1)
    session['national_rank_data_df']["swimdate"] = session['national_rank_data_df']["swimdate"].apply(lambda x: x.strftime('%m/%d/%Y'))
    #session['national_rank_data_df']['age'] = session['national_rank_data_df']['age'].apply(lambda x: get_age_group(x))
    session['national_rank_data_df']['age group'] = session['national_rank_data_df']['age'].apply(lambda x: age_group_str(get_age_group(x)))
    session['national_rank_data_df'].rename(columns={'rank': 'national rank'}, inplace=True)
    session['national_rank_data_df'].drop('relay', axis=1, inplace=True)
    session['national_rank_data_df']['lsc rank'] = session['national_rank_data_df'].groupby(['sex', 'age group', 'event', 'lsc'])['swimtime'].rank(method='dense', ascending=True)
    session['national_rank_data_df']['club rank'] = session['national_rank_data_df'].groupby(['sex', 'age group', 'event', 'team'])['swimtime'].rank(method='dense', ascending=True)
    """
    with session['main_page_column']:
        ui.label('SwimRank').style('font-size: 200%; font-weight: 300, font-family: "Times New Roman", Times, serif;')
        session['search_input'] = ui.input(label='Enter name...', placeholder='Type a name...')
        session['search_input'].on('keypress.enter', lambda: fetch_people(session['search_input'].value))
        
        session['id_table'] = ui.table(rows=[], columns=[])
        session['id_table'].visible = False
        if not session['id_table_df'].empty:
            await update_id_table()
        


# Function to update visible table
async def show_page(PAGE_SIZE=20):
    await ui.context.client.connected()
    session = app.storage.tab
    
    total_pages = math.ceil(len(session['rank_df_copied']) / PAGE_SIZE)
    if session['current_ranking_page']['num'] >= total_pages:
        session['next_rank_page'].visible = False
    else:
        session['next_rank_page'].visible = True
    if session['current_ranking_page']['num'] == 1:
        session['previous_rank_page'].visible = False
    else:
        session['previous_rank_page'].visible = True
    start = (session['current_ranking_page']['num'] - 1) * PAGE_SIZE
    end = start + PAGE_SIZE
    page_df = session['rank_df_copied'].iloc[start:end]
    session['ranking_table_container'].clear()
    if page_df.empty:
        ui.label("No data matches these filters.").style('color: gray; font-size: 18px;').classes('mt-4')
    else:
        session['ranking_table'].columns = [
        {'name': 'event', 'label': 'Event', 'field': 'event'},
        {'name': 'age group', 'label': 'Age Group', 'field': 'age group'},
        {'name': 'team', 'label': 'Team', 'field': 'team'},
        {'name': 'meet', 'label': 'Meet', 'field': 'meet'},
        {'name': 'swimdate', 'label': 'Swim Date', 'field': 'swimdate'},
        {'name': 'swimtime', 'label': 'Swim Time', 'field': 'swimtime'},
        {'name': 'national rank', 'label': 'National Rank', 'field': 'national rank'},
        {'name': 'lsc rank', 'label': 'LSC Rank', 'field': 'lsc rank'},
        {'name': 'club rank', 'label': 'Club Rank', 'field': 'club rank'}
    ]
        session['ranking_table'].rows = page_df.to_dict('records')
        session['ranking_table'].visible = True

    # Pagination controls
async def next_page(PAGE_SIZE=20):
    await ui.context.client.connected()
    session = app.storage.tab
    total_pages = math.ceil(len(session['rank_df_copied']) / PAGE_SIZE)
    if session['current_ranking_page']['num'] < total_pages:
        session['current_ranking_page']['num'] += 1
        await show_page()

async def prev_page():
    await ui.context.client.connected()
    session = app.storage.tab
    if session['current_ranking_page']['num'] > 1:
        session['current_ranking_page']['num'] -= 1
        await show_page()


# Filtering logic
async def refresh_table(df):
    await ui.context.client.connected()
    session = app.storage.tab
    rt = session['rank_type_select'].value
    sex = 0 if session['sex_select'].value == 'Male' else 1
    ev = session['event_select'].value
    ag = session['age_select'].value
    ls = session['lsc_select'].value
    cl = session['club_select'].value
    session['rank_df_copied'] = df.copy()
    session['rank_df_copied'] = session['rank_df_copied'][session['rank_df_copied']['event'] == ev]        
    session['rank_df_copied'] = session['rank_df_copied'][session['rank_df_copied']['age group'] == ag]
    session['rank_df_copied'] = session['rank_df_copied'][session['rank_df_copied']['sex'] == int(sex)]
    if rt == 'lsc' and ls:
        session['rank_df_copied'] = session['rank_df_copied'][session['rank_df_copied']['lsc'] == ls]
    if rt == 'club' and cl:
        session['rank_df_copied'] = session['rank_df_copied'][session['rank_df_copied']['team'] == cl]

    rank_col = f'{rt} rank'
    if rank_col in session['rank_df_copied'].columns:
        session['rank_df_copied'] = session['rank_df_copied'].sort_values(by=rank_col)
    session['current_ranking_page']['num'] = 1
    await show_page()

@ui.page('/rankings')
async def rankings_page(rank_type: str = 'national', event = '50 FR SCY', age_group = '17-18', lsc = '', club = '', sex: int = 0):
    await ui.context.client.connected()
    session = app.storage.tab
    # We assume this DataFrame is already populated globally
    df = session['national_rank_data_df']
    if df is None or df.empty:
        ui.label("No ranking data loaded. Please run the season summary first.").style('font-size: 20px; color: gray;')
        return

    # Normalize column names (ensure lowercase)
    df.columns = [c.lower().strip() for c in df.columns]
    # Available filters
    all_events = sorted(df['event'].unique().tolist())
    all_sex= ['Male', 'Female']
    all_age_groups = sorted(df['age group'].unique().tolist())
    all_lscs = sorted(df['lsc'].dropna().unique().tolist())
    all_clubs = sorted(df['team'].dropna().unique().tolist())
    # Filter DataFrame initially
    session['rank_df_copied'] = df.copy()
    session['rank_df_copied'] = session['rank_df_copied'][session['rank_df_copied']['event'] == event]
    session['rank_df_copied'] = session['rank_df_copied'][session['rank_df_copied']['sex'] == sex]

    session['rank_df_copied'] = session['rank_df_copied'][session['rank_df_copied']['age group'] == age_group]
    if rank_type == 'lsc' and lsc:
        session['rank_df_copied'] = session['rank_df_copied'][session['rank_df_copied']['lsc'] == lsc]
    if rank_type == 'club' and club:
        session['rank_df_copied'] = session['rank_df_copied'][session['rank_df_copied']['team'] == club]

    # Sort by rank type column
    rank_col = f'{rank_type} rank'
    if rank_col in session['rank_df_copied'].columns:
        session['rank_df_copied'] = session['rank_df_copied'].sort_values(by=rank_col)
    else:
        ui.label(f"Rank column '{rank_col}' not found in DataFrame.").style('color: red; font-size: 16px;')
        return
    # Reactive variables
    session['current_ranking_page'] = {'num': 1}
    PAGE_SIZE = 20

    # Dropdowns row
    with ui.row().classes('items-center justify-center gap-4 mt-4'):
        session['rank_type_select'] = ui.select(
            options=['national', 'lsc', 'club'],
            value=rank_type,
            label='Rank Type',
            on_change=lambda: refresh_table(df)
        )

        session['sex_select'] = ui.select(
            options=all_sex,
            value='Male' if sex == 0 else 'Female',
            label='Sex',
            on_change=lambda: refresh_table(df)
        )

        session['event_select'] = ui.select(
            options=all_events,
            value=event if event in all_events else all_events[0],
            label='Event',
            on_change=lambda: refresh_table(df)
        )

        session['age_select'] = ui.select(
            options=all_age_groups,
            value=age_group if age_group in all_age_groups else all_age_groups[0],
            label='Age Group',
            on_change=lambda: refresh_table(df)
        )

        # Only show LSC/club selector dynamically
        session['lsc_select'] = ui.select(
            options=all_lscs,
            value=lsc if lsc in all_lscs else None,
            label='LSC',
            on_change=lambda: refresh_table(df)
        ).bind_visibility_from(session['rank_type_select'], 'value', backward=lambda v: v == 'lsc')

        session['club_select'] = ui.select(
            options=all_clubs,
            value=club if club in all_clubs else None,
            label='Team',
            with_input=True,
            on_change=lambda: refresh_table(df)
        ).props('dense outlined clearable').bind_visibility_from(session['rank_type_select'], 'value', backward=lambda v: v == 'club')

    ui.separator()
    # Container for table
    session['ranking_table_container'] = ui.column().classes('w-full items-center')
    session['ranking_table'] = ui.table(rows=[], columns=[])
    session['ranking_table'].visible = False

    
    with ui.row():#.classes('items-center justify-center gap-4 mt-4'):
        session['previous_rank_page'] = ui.button('Previous', on_click=lambda: prev_page())
        session['previous_rank_page'].visible = False
        ui.label().bind_text_from(session['current_ranking_page'], 'num', backward=lambda v: f"Page {v}")
        session['next_rank_page'] = ui.button('Next', on_click=lambda ps = PAGE_SIZE: next_page(ps))

    await show_page()



if __name__ in {"__main__", "__mp_main__"}:
    ui.run(title='SwimRank')
