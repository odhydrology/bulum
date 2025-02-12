import pandas as pd
import altair as alt
import folium


def simple_station_map(centroid_coords, area_geojson, station_df: pd.DataFrame, zoom_level=10, extra_geojson1=None, extra_geojson2=None):
    """Returns a map of stations with pop up of information provided for each station

    Args:
        centroid_coords (array of doubles): centroid coordinates for your map
        area_geojson (geojson layer): outline layer for your map (i.e., catchment, reach, state)
        station_df (pd.DataFrame): dataframe with station details. Must include 'Station name', 'Station number', 'Latitude' and 'Longitude'. Any other columns are fine and information will be included in map pop up.
        zoom_level (int, optional): zoom level for your map. Defaults to 10.
        extra_geojson1 (geojson layer, optional): additional mapping layer. Defaults to None.
        extra_geojson2 (geojson layer, optional): additional mapping layer. Defaults to None.

    Returns:
        folium map: map of stations
    """

    # Set up map
    # My preferred tiles are 'Stamen Terrain', but they are no longer available. Might be available through StadiaMap, but haven't figured that out yet.
    m = folium.Map(location=centroid_coords, zoom_start=zoom_level, tiles="OpenStreetMap")
    
    # Add additional geojson map layers if the user has specified them
    if extra_geojson2 is None:
        pass
    else:
        folium.GeoJson(extra_geojson2, 
                name='geojson', 
                style_function=lambda x: {'color': '#12239e', 'weight': 0.5}).add_to(m)

    if extra_geojson1 is None:
        pass
    else:
        folium.GeoJson(extra_geojson1, 
                name='geojson', 
                style_function=lambda x: {'color': '#12239e', 'weight': 0.5}).add_to(m)
    
    # Add area 
    folium.GeoJson(area_geojson, 
                name='geojson', 
                style_function=lambda x: {'color': '#b12ef7', 'weight': 3}).add_to(m)

    # Add markers for the stations
    for i in range(0,len(station_df)):
        row = station_df.iloc[i]
        number_name = f"{row['Station number']} ({row['Station name']})"
        location = [station_df.iloc[i]['Latitude'], station_df.iloc[i]['Longitude']]
        
        # Add information from station data frame into marker pop up
        stationInfo=''
        for c in range(0,len(station_df.columns)):
            stationInfo+=("<b>" + station_df.columns[c] + ":</b> " + f"{row[station_df.columns[c]]}<br>")
        stationPopup=folium.Popup(stationInfo,min_width=100,max_width=400)

        folium.Marker(
            location=location,
            popup=stationPopup,
            icon=folium.Icon(icon='circle', prefix='fa', color='blue'),
            tooltip=number_name
        ).add_to(m)
    return m


def station_map_plots(centroid_coords, area_geojson, station_df: pd.DataFrame, data_df, data_label='data', zoom_level=10):
    """Returns a map of markers with pop up of daily data plot

    Args:
        centroid_coords (array of doubles):centroid coordinates for your map
        area_geojson (geojson layer): outline layer for your map (i.e., catchment, reach, state)
        station_df (pd.DataFrame): dataframe with station details. Must include 'Name', 'Latitude' and 'Longitude'. Any other columns ignored.
        data_df (array of pd.DataFrame): daily data to plot in pop up
        data_label (str, optional): description of data, used in plot title. Defaults to 'data'.
        zoom_level (int, optional): zoom level for your map. Defaults to 10.

    Returns:
        _type_: map of stations with daily data plots
    """

    # Set up map
    # My preferred tiles are 'Stamen Terrain', but they are no longer available. Might be available through StadiaMap, but haven't figured that out yet.
    m = folium.Map(location=centroid_coords, zoom_start=zoom_level, tiles="OpenStreetMap")
    
    # Add area 
    folium.GeoJson(area_geojson, 
                name='geojson', 
                style_function=lambda x: {'color': '#b12ef7', 'weight': 3}).add_to(m)

    # Add markers for the stations
    for i in range(0,len(station_df)):
        row = station_df.iloc[i]
        name = f"{row['Name']}"
        location = [station_df.iloc[i]['Latitude'], station_df.iloc[i]['Longitude']]
        
        # Add daily data to plot and put in pop up
        alt.data_transformers.disable_max_rows()
        dataChart = alt.Chart(data_df[i]).mark_line().encode(
            alt.X('Date:T'),
            alt.Y(data_df[i].columns[1] + ':Q',
            axis=alt.Axis(title=data_label))
        ).properties(width=600,height=300,title=name).interactive()

        stationPopup=folium.Popup(min_width=100,max_width=700).add_child(folium.features.VegaLite(dataChart,height=350,width=700))

        folium.Marker(
            location=location,
            popup=stationPopup,
            icon=folium.Icon(icon='circle', prefix='fa', color='blue'),
            tooltip=name
        ).add_to(m)
    return m