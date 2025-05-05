import json
from pathlib import Path
from datetime import datetime

def generate_html_report(results, output_file):
    """Generate an HTML report from test results."""
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Supabase Test Report</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; }}
            .test {{ margin-bottom: 20px; padding: 10px; border: 1px solid #ddd; }}
            .passed {{ background-color: #d4edda; }}
            .failed {{ background-color: #f8d7da; }}
            .details {{ margin-top: 10px; }}
            .error {{ color: #721c24; }}
        </style>
    </head>
    <body>
        <h1>Supabase Test Report</h1>
        <p>Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    """

    for test_name, result in results.items():
        status_class = 'passed' if result.get('passed', False) else 'failed'
        html_content += f"""
        <div class="test {status_class}">
            <h2>{test_name.replace('_', ' ').title()}</h2>
            <p>Status: {'Passed' if result.get('passed', False) else 'Failed'}</p>
            <div class="details">
                <p>{result.get('details', '')}</p>
                {f'<p class="error">Error: {result.get("error", "")}</p>' if result.get('error') else ''}
            </div>
        </div>
        """

    html_content += """
    </body>
    </html>
    """

    with open(output_file, 'w') as f:
        f.write(html_content)

def save_test_results(results, output_file):
    """Save test results to a JSON file."""
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2) 