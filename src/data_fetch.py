import pandas as pd
from nba_api.stats.endpoints import LeagueGameLog

# Parámetros por defecto de la temporada actual
DEFAULT_SEASON = "2025-26"
DEFAULT_SEASON_TYPE = "Regular Season"


def get_current_season_team_logs(
    season: str = DEFAULT_SEASON,
    season_type: str = DEFAULT_SEASON_TYPE,
) -> pd.DataFrame:
    """
    Descarga los logs de PARTIDO a nivel EQUIPO usando la API oficial de la NBA
    (nba_api) y devuelve un DataFrame con las columnas necesarias.

    Cada fila = 1 equipo en 1 partido.
    """
    logs = LeagueGameLog(
        season=season,
        season_type_all_star=season_type,
        player_or_team_abbreviation="T",  # 'T' = team
    )

    df = logs.get_data_frames()[0].copy()

    # Estandarizamos la fecha
    df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"])

    # Nos quedamos solo con las columnas que vamos a usar
    cols = [
        "SEASON_ID",
        "TEAM_ID",
        "TEAM_ABBREVIATION",
        "TEAM_NAME",
        "GAME_ID",
        "GAME_DATE",
        "MATCHUP",
        "WL",
        "PTS",
        "FGA",
        "FTA",
        "OREB",
        "TOV",
    ]
    df = df[cols].copy()

    return df


def build_team_level_dataset(df_logs: pd.DataFrame) -> pd.DataFrame:
    """
    A partir de los logs de la API construye un dataset a nivel EQUIPO-PARTIDO,
    con:
      - IS_HOME
      - POINTS_FOR / POINTS_AGAINST
      - MARGIN
      - WIN
      - columnas de tiro (FGA, FTA, OREB, TOV)
    """
    df = df_logs.copy()

    # Orden por equipo y fecha
    df = df.sort_values(["TEAM_ID", "GAME_DATE"]).reset_index(drop=True)

    # Flag local/visitante: en MATCHUP 'vs.' = local, '@' = visitante
    df["IS_HOME"] = df["MATCHUP"].str.contains(" vs. ").astype(int)

    # Puntos a favor
    df["POINTS_FOR"] = df["PTS"]

    # Puntos en contra: hacemos un self-merge por GAME_ID
    opp = df[["GAME_ID", "TEAM_ID", "POINTS_FOR"]].rename(
        columns={
            "TEAM_ID": "OPP_TEAM_ID",
            "POINTS_FOR": "OPP_POINTS_FOR",
        }
    )

    df = df.merge(opp, on="GAME_ID")
    df = df[df["TEAM_ID"] != df["OPP_TEAM_ID"]].copy()

    # Nos quedamos con una fila por TEAM_ID-GAME_ID
    df.drop(columns=["OPP_TEAM_ID"], inplace=True)
    df["POINTS_AGAINST"] = df["OPP_POINTS_FOR"]
    df.drop(columns=["OPP_POINTS_FOR"], inplace=True)

    # Margen y victoria
    df["MARGIN"] = df["POINTS_FOR"] - df["POINTS_AGAINST"]
    df["WIN"] = (df["MARGIN"] > 0).astype(int)

    # Dejamos columnas ordenadas
    ordered_cols = [
        "GAME_ID",
        "GAME_DATE",
        "TEAM_ID",
        "TEAM_ABBREVIATION",
        "TEAM_NAME",
        "IS_HOME",
        "POINTS_FOR",
        "POINTS_AGAINST",
        "WIN",
        "MARGIN",
        "FGA",
        "FTA",
        "OREB",
        "TOV",
        "MATCHUP",
        "SEASON_ID",
    ]
    df = df[ordered_cols].copy()

    return df


def build_game_level_dataset(
    df_team: pd.DataFrame,
    max_games: int | None = None,
) -> pd.DataFrame:
    """
    Construye un dataset a nivel PARTIDO (una fila por GAME_ID) con info
    de equipo local y visitante (puntos, nombres, etc.).
    """
    df = df_team.copy()

    # Opcional: limitar al primer N partidos de la temporada (por fecha)
    if max_games is not None:
        game_ids = (
            df.sort_values("GAME_DATE")["GAME_ID"]
            .drop_duplicates()
            .iloc[:max_games]
        )
        df = df[df["GAME_ID"].isin(game_ids)]

    # HOME
    home = (
        df[df["IS_HOME"] == 1]
        .rename(
            columns={
                "TEAM_ID": "HOME_TEAM_ID",
                "TEAM_ABBREVIATION": "HOME_TEAM_ABBR",
                "TEAM_NAME": "HOME_TEAM_NAME",
                "POINTS_FOR": "HOME_PTS",
                "POINTS_AGAINST": "HOME_PA",
                "MARGIN": "HOME_MARGIN",
            }
        )
        [
            [
                "GAME_ID",
                "GAME_DATE",
                "HOME_TEAM_ID",
                "HOME_TEAM_ABBR",
                "HOME_TEAM_NAME",
                "HOME_PTS",
                "HOME_PA",
                "HOME_MARGIN",
            ]
        ]
    )

    # AWAY
    away = (
        df[df["IS_HOME"] == 0]
        .rename(
            columns={
                "TEAM_ID": "AWAY_TEAM_ID",
                "TEAM_ABBREVIATION": "AWAY_TEAM_ABBR",
                "TEAM_NAME": "AWAY_TEAM_NAME",
                "POINTS_FOR": "AWAY_PTS",
                "POINTS_AGAINST": "AWAY_PA",
                "MARGIN": "AWAY_MARGIN",
            }
        )
        [
            [
                "GAME_ID",
                "AWAY_TEAM_ID",
                "AWAY_TEAM_ABBR",
                "AWAY_TEAM_NAME",
                "AWAY_PTS",
                "AWAY_PA",
                "AWAY_MARGIN",
            ]
        ]
    )

    games = home.merge(away, on="GAME_ID")

    # Targets principales
    games["HOME_WIN"] = (games["HOME_PTS"] > games["AWAY_PTS"]).astype(int)
    games["MARGIN_HOME"] = games["HOME_PTS"] - games["AWAY_PTS"]
    games["TOTAL_POINTS"] = games["HOME_PTS"] + games["AWAY_PTS"]

    # Ordenamos cronológicamente
    games = games.sort_values("GAME_DATE").reset_index(drop=True)

    return games
