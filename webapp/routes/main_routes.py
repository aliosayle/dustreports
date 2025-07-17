"""Main page routes"""

from flask import Blueprint, render_template

main_bp = Blueprint('main', __name__)

@main_bp.route('/')
def index():
    return render_template('index.html')

@main_bp.route('/autonomy')
def autonomy():
    return render_template('autonomy.html')

@main_bp.route('/custom-reports')
def custom_reports():
    return render_template('custom_reports.html')

@main_bp.route('/stock-by-site')
def stock_by_site():
    return render_template('stock_by_site.html')

@main_bp.route('/ciment-report')
def ciment_report():
    """Render the Ciment Report page"""
    return render_template('ciment_report.html')

@main_bp.route('/sales-report')
def sales_report():
    """Render the Sales Report page"""
    return render_template('sales_report.html')

@main_bp.route('/favicon.ico')
def favicon():
    # Redirect to static favicon file
    from flask import redirect, url_for
    return redirect(url_for('static', filename='favicon.ico'))
