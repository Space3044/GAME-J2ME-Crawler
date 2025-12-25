import pandas as pd
import os
import io


def export_task_to_excel(task_data):
    """
    Converts task data to an Excel file bytes object.
    """
    data_list = task_data.get('data', [])

    if not data_list:
        return None

    df = pd.DataFrame(data_list)

    # Reorder columns if needed, or ensure specific columns exist
    expected_columns = ['ID', 'Title', 'URL', 'Description']
    # Filter to only include expected columns if they exist, or all if not
    cols = [c for c in expected_columns if c in df.columns]
    # Add any other columns that might be in the data
    cols += [c for c in df.columns if c not in expected_columns]

    df = df[cols]

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Games')

    output.seek(0)
    return output


def generate_filename(task_data):
    # Get the original filename without extension
    json_filename = task_data.get('filename', 'export')
    if json_filename.endswith('.json'):
        base_name = json_filename[:-5]
    else:
        base_name = json_filename

    # Get created_at date part (YYYYMMDD)
    created_at = task_data.get('created_at', '')
    if '_' in created_at:
        date_part = created_at.split('_')[0]
    else:
        # Fallback
        import datetime
        date_part = datetime.datetime.now().strftime('%Y%m%d')

    return f"{base_name}_{date_part}.xlsx"
