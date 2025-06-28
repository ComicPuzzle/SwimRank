import math
import time
from nicegui import ui, app
import pandas as pd
import asyncio
import asyncpg
from nicegui.events import KeyEventArguments
from datetime import datetime, timedelta


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
    #global pool, control_pressed, c_pressed, control_timer
    #print(e)
    if e.modifiers.ctrl and e.action.keydown:
        print("Ctrl pressed")
        session.control_timer = time.time()
    elif e.key == 'c' and e.action.keyup:
        print("c pressed")
        if time.time() - session.control_timer < 0.5:
            await session.pool.close()
            app.shutdown()  # Stop the NiceGUI application

async def create_pool(pool, dbname, port, password):
    try:
        await pool.close()
    except:
        pass
    return await asyncpg.create_pool(dsn=f'postgres://postgres:{password}@localhost:{port}/{dbname}', max_inactive_connection_lifetime=20)

async def fetch_people(name, id_table, id_table_df, pool):
    name = name.lower().strip().split()
    split_name = ""
    for part in name:
        split_name += '%' + part
    split_name += '%'
    query = f"""SELECT * FROM "ResultsSchema"."SwimmerIDs" WHERE LOWER("Name") LIKE '{split_name}'"""
    
    if pool.is_closing():
        pool = await create_pool()
    async with pool.acquire() as con:
        rows = await con.fetch(query)
    id_table_df = pd.DataFrame(rows, columns=['Name', 'Age', 'LSC', 'Club', 'PersonKey', 'Sex'])
    #print(id_table_df.head())
    await update_id_table(id_table, id_table_df)

async def fetch_person_event_data(table, key):
    if session.pool.is_closing():
        await create_pool()
    async with session.pool.acquire() as con:
        query = f"""SELECT "event", "swimtime", "relay", "age", "points", "timestandard",  
                            "meet", "team", "swimdate" FROM "ResultsSchema"."{table}" WHERE "personkey" = {key}"""
        rows = await con.fetch(query)
        return rows

async def update_id_table(id_table, id_table_df):
    id_table.columns = [{'name': col, 'label': col, 'field': col} for col in id_table_df.columns]
    id_table.rows = id_table_df.to_dict('records')

    id_table.add_slot('body-cell-Name', """
        <q-td :props="props">
            <q-btn @click="() => $parent.$emit('person_selected', props.row)" 
                    :label="props.row.Name" 
                    flat dense color='primary'/>
        </q-td>
    """)
    id_table.update()
    id_table.visible = True
    id_table.on('person_selected', lambda msg: ui.navigate.to(f'/swimmer/{msg.args["PersonKey"]}'))

async def update_results_table(course):
    #global event_results_table, lcm_df, scy_df
    if course == "SCY":
        session.event_results_table.columns = [{'name': col, 'label': col, 'field': col} for col in session.scy_df.columns]
        session.event_results_table.rows = session.scy_df.to_dict('records')
    else:
        session.event_results_table.columns = [{'name': col, 'label': col, 'field': col} for col in session.lcm_df.columns]
        session.event_results_table.rows = session.lcm_df.to_dict('records')
    session.event_results_table.visible = True
    session.event_results_table.update()
    #add graph

async def update_upcoming_meets_table(upcoming_meets_table):
    upcoming_meets_table.visible = True
    upcoming_meets_table.update()

async def update_ncaa_comparison_table(ncaa_comparison_table):
    ncaa_comparison_table.visible = True
    ncaa_comparison_table.update()

async def update_season_rankings_table(season_rankings_table):
    season_rankings_table.visible = True
    season_rankings_table.update()

async def update_best_rankings_table():
    #global best_rankings_table, lcm_df, scy_df
    scy_copy = session.scy_df.copy()
    lcm_copy = session.lcm_df.copy()
    scy_copy['swimtime'] = scy_copy['swimtime'].apply(lambda x: str_to_datetime(x.replace('r', "")))
    lcm_copy['swimtime'] = lcm_copy['swimtime'].apply(lambda x: str_to_datetime(x.replace('r', "")))
    scy_min_row = session.scy_df.loc[scy_copy['swimtime'].idxmin()].to_dict()
    lcm_min_row = session.lcm_df.loc[lcm_copy['swimtime'].idxmin()].to_dict()
    session.best_rankings_table.columns = [{'name': col, 'label': col, 'field': col} for col in ["event", "swimtime","age", "points", "timestandard", "meet", "team", "swimdate"]]
    session.best_rankings_table.rows = [scy_min_row, lcm_min_row]
    session.best_rankings_table.visible = True
    session.best_rankings_table.update()

async def update_progression_chart():
    #global chart, lcm_df, scy_df
    # 1) prepare SCY series
    def get_series(df):
        df_copy = df.copy()
        df_copy['parsed_time'] = df_copy['swimtime'].apply(lambda x: str_to_timedelta(x.replace("r", "")))
        df_copy['parsed_date'] = pd.to_datetime(df_copy['swimdate'], format='%m/%d/%Y')
        df_copy.sort_values('parsed_date', inplace=True)
        return df_copy.apply(
            lambda row: [ row['parsed_date'].strftime('%Y-%m-%d'),
                        row['parsed_time'].total_seconds()],
            axis=1
        ).tolist()

    scy_series = get_series(session.scy_df)
    lcm_series = get_series(session.lcm_df)
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
        'series': [
            {
                'name': 'SCY',
                'type': 'line',
                'data': scy_series,
                'smooth': False,
                'itemStyle': { 'color': 'blue' },
                'lineStyle': { 'width': 3 },
                'symbolSize': 6,
            },
            {
                'name': 'LCM',
                'type': 'line',
                'data': lcm_series,
                'smooth': False,
                'itemStyle': { 'color': 'red' },
                'lineStyle': { 'width': 3 },
                'symbolSize': 6,
            }
        ]
    }
    if session.chart:
        session.chart.delete()
    session.chart = ui.echart(options=option).style('height: 600px; width: 100%; min-height: 600px;')
    session.chart.visible = True


async def collect_all_event_data(person_key):
    db_table_names = ['50_FR_SCY_results', '50_FR_LCM_results', '100_FR_SCY_results', '100_FR_LCM_results',
                        '200_FR_SCY_results', '200_FR_LCM_results', '400_FR_LCM_results', '500_FR_SCY_results', 
                        '800_FR_LCM_results', '1000_FR_SCY_results', '1500_FR_LCM_results', '1650_FR_SCY_results',
                        '50_BK_SCY_results', '100_BK_SCY_results', '200_BK_SCY_results', '50_BK_LCM_results', '100_BK_LCM_results', '200_BK_LCM_results',
                        '50_FL_SCY_results', '100_FL_SCY_results', '200_FL_SCY_results', '50_FL_LCM_results', '100_FL_LCM_results', '200_FL_LCM_results',
                        '50_BR_SCY_results', '100_BR_SCY_results', '200_BR_SCY_results', '50_BR_LCM_results', '100_BR_LCM_results', '200_BR_LCM_results',
                        '100_IM_SCY_results', '200_IM_SCY_results', '400_IM_SCY_results', '200_IM_LCM_results', '400_IM_LCM_results'
                        ]
    semaphore = asyncio.Semaphore(5)  # Limit concurrent queries to 5
    tasks = []

    async def fetch_with_semaphore(table):
        async with semaphore:
            return await fetch_person_event_data(table, person_key)

    for table in db_table_names:
        tasks.append(fetch_with_semaphore(table))

    # Use asyncio.gather to collect all results
    results = await asyncio.gather(*tasks)
    
    # Flatten the list of lists
    all_event_data = [item for sublist in results for item in sublist]

    return all_event_data

async def display_event_data(e, event_df):
    #global lcm_df, scy_df, event_results_table, event_label 
    #global best_times_label, course_radio, meets_label
    #global season_rankings_label, career_rankings_label, progress_label, ncaa_comparison_label
    #global comparison_label, results_column, upcoming_meets_table, season_rankings_table, best_rankings_table, ncaa_comparison_table
    #global best_times_column, upcoming_meets_column, season_rankings_column, ncaa_comparison_column, chart
    lcm_df = event_df.loc[event_df['event'].str.contains("LCM")]
    scy_df = event_df.loc[event_df['event'].str.contains("SCY")]
    if not results_column:
        best_times_column = ui.column().classes('w-full items-center')
        ncaa_comparison_column = ui.column().classes('w-full items-center')
        upcoming_meets_column = ui.column().classes('w-full items-center')
        season_rankings_column = ui.column().classes('w-full items-center')
        results_column = ui.column().classes('w-full items-center')
    
    with best_times_column:
        if not best_times_label:
            best_times_label = ui.label('Best Times').style('font-size: 28px')
        try:
            best_rankings_table.delete()
        except:
            pass
        best_rankings_table = ui.table(rows=[])
        best_rankings_table.visible = False
        await update_best_rankings_table()
    with ncaa_comparison_column:
        if not ncaa_comparison_label:
            ncaa_comparison_label = ui.label('NCAA Comparison')
        try:
            ncaa_comparison_table.delete()
        except:
            pass
        ncaa_comparison_table = ui.table(rows=[])
        ncaa_comparison_table.visible = False
        await update_ncaa_comparison_table()
    with upcoming_meets_column:
        if not meets_label:
            meets_label = ui.label('Upcoming Championship Meets')
        try:
            upcoming_meets_table.delete()
        except:
            pass
        upcoming_meets_table = ui.table(rows=[])
        upcoming_meets_table.visible = False
        await update_upcoming_meets_table()
    with season_rankings_column: 
        if not season_rankings_label:
            season_rankings_label = ui.label('Current Season Rankings')
        try:
            season_rankings_table.delete()
        except:
            pass
        season_rankings_table = ui.table(rows=[])
        season_rankings_table.visible = False
        await update_season_rankings_table()
    with results_column:
        if not event_label: 
            event_label = ui.label(e + " Progression") 
        if not course_radio:
            course_radio = ui.radio(["SCY", "LCM"], value="SCY", on_change=lambda: update_results_table(session.course_radio.value)).props('inline')
            print('set radio')
        try:
            event_results_table.delete()
        except:
            pass
        event_results_table = ui.table(rows=[])
        event_results_table.visible = False
        await update_results_table(course_radio.value)
        try:
            chart.delete()
        except:
            pass
        chart = ui.echart([])
        chart.visible = False
        await update_progression_chart()
    return scy_df, lcm_df
        

@ui.page('/swimmer/{person_key}')
async def graph_page(person_key: str):
    await ui.context.client.connected()
    session = app.storage.tab
    #global all_event_data_df, scy_df, lcm_df, keyboard, event_label
    #global best_times_label, course_radio, meets_label, season_rankings_label
    #global career_rankings_label, progress_label, comparison_label, results_column
    #global best_times_column, upcoming_meets_column, season_rankings_column
    #global ncaa_comparison_column, ncaa_comparison_label, chart, previous_personkey
    session['keyboard'] = ui.keyboard(on_key=handle_key)
    try:
        if session['pool'].is_closing():
            session['pool'] = await create_pool(session['pool'], session['dbname'], session['port'], session['password'])
    except:
        session['pool'] = await create_pool(session['pool'], session['dbname'], session['port'], session['password'])
    async with session['pool'].acquire() as con:
        query = f"""SELECT "Name", "Age", "LSC", "Club", "Sex" FROM "ResultsSchema"."SwimmerIDs" WHERE "PersonKey" = {person_key}"""
        row = await con.fetchrow(query)
    name = row[0]
    age = row[1]
    lsc = row[2]
    club = row[3]
    sex = "Male" if row[4] == 0 else "Female"
    
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
        #session['course_radio'], session['best_times_label'], session['meets_label'], session.season_rankings_label, session.career_rankings_label = None, None, None, None, None
        #session['progress_label'], session['comparison_label'], session['results_column'], session.best_times_column = None, None, None, None
        #session['upcoming_meets_column'], session['season_rankings_column']. session.ncaa_comparison_column = None, None, None
        #session['ncaa_comparison_label'], session['chart'], session['event_label'] = None, None, None
        all_event_data = await collect_all_event_data(person_key)
        session['all_event_data_df'] = pd.DataFrame(all_event_data, columns=["event", "swimtime", "relay", 
                                                                "age", "points", "timestandard",  
                                                                "meet", "team", "swimdate"])
        session['all_event_data_df'].sort_values(by='swimdate', inplace=True, ascending=False)
        print(session.all_event_data_df)
        session['all_event_data_df']["swimtime"] = session['all_event_data_df'].apply(lambda row: convert_timedelta(row['swimtime']) + "r" if row['relay'] == 1 else convert_timedelta(row['swimtime']), axis=1)
        session['all_event_data_df']["swimdate"] = session['all_event_data_df']["swimdate"].apply(lambda x: x.strftime('%m/%d/%Y'))
        session['all_event_data_df'].drop('relay', axis=1, inplace=True)

        first_non_empty_event, first_non_empty_event_df = await make_event_buttons(session['all_event_data_df'])
        session['scy_df'], session['lcm_df'] = await display_event_data(first_non_empty_event, first_non_empty_event_df)
    else:
        event = session.scy_df['event'].iloc[0].split('SCY')[0].strip()
        e = session.all_event_data_df[session.all_event_data_df['event'].str.contains(event)]
        await make_event_buttons()
        session.results_column.delete()
        session.best_times_column.delete() 
        session.upcoming_meets_column.delete()
        session.season_rankings_column.delete()
        session.ncaa_comparison_column.delete()
        session.course_radio, session.meets_label, session.season_rankings_label, session.career_rankings_label = None, None, None, None
        session.progress_label, session.comparison_label, session.results_column, session.best_times_column = None, None, None, None
        session.upcoming_meets_column, session.season_rankings_column, session.ncaa_comparison_column = None, None, None
        session.best_times_label, session.ncaa_comparison_label, session.chart, session.event_label = None, None, None, None
        await display_event_data(event, e)

@ui.page('/')
async def main_page():
    await ui.context.client.connected()
    session = app.storage.tab
    session['id_table_df'] = pd.DataFrame()
    session['id_table'] = None
    session['pool'] = None
    session['previous_personkey'] = None

    with open('credentials.txt', 'r') as file:
        arr = []
        for line in file:
            arr.append(line.strip())
        session['dbname'] = arr[0]
        session['port'] = arr[1]
        session['password'] = arr[2]

    session['pool'] = await create_pool(session['pool'], session['dbname'], session['port'], session['password'])
    session['keyboard'] = ui.keyboard(on_key=handle_key)
    session['main_page_column'] = ui.column().classes('w-full items-center')
    with session['main_page_column']:
        ui.label('SwimRank').style('font-size: 200%; font-weight: 300, font-family: "Times New Roman", Times, serif;')
        session['search_input'] = ui.input(label='Enter name...', placeholder='Type a name...')
        session['search_input'].on('keypress.enter', lambda: fetch_people(session['search_input'].value, session['id_table'],
                                                                          session['id_table_df'], session['pool']))
        
        session['id_table'] = ui.table(rows=[], columns=[])
        session['id_table'].visible = False
        if not session['id_table_df'].empty:
            await update_id_table(session['id_table'], session['id_table_df'])
        
if __name__ in {"__main__", "__mp_main__"}:
    ui.run(title='SwimRank')
