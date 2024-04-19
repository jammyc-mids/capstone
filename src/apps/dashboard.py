from flask import Flask, render_template, request, redirect, url_for
import dash
import dash_core_components as dcc
import dash_html_components as html
import subprocess

server = Flask(__name__, static_url_path='/assets', static_folder='assets')

@server.route('/')
def index():
    return redirect(url_for('login'))

@server.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        user_type = request.form['user_type']
        if user_type == 'residential':
            return redirect(url_for('residential_dashboard'))
        elif user_type == 'analyst':
            return redirect(url_for('energy_analyst_dashboard'))
    return render_template('login.html')

@server.route('/logout')
def logout():
    return redirect(url_for('login'))

@server.route('/residential_dashboard')
def residential_dashboard():
    return render_template('residential_dashboard.html')

def get_EC2_public_address(instance):
    acmd = f"aws ec2 describe-instances --filters \"Name=tag:Name,Values={instance}\" \
        --query 'Reservations[*].Instances[*].PublicDnsName' --output text"
    p = subprocess.Popen(acmd, stdout=subprocess.PIPE, shell=True)
    output,error = p.communicate()
    return output.decode().splitlines()[0]

@server.route('/energy_analyst_dashboard')
def energy_analyst_dashboard():
    return render_template('index.html', \
        ip_address=get_EC2_public_address('csprod-infra-01'))

if __name__ == '__main__':

    server.run(host="0.0.0.0", port=5001, debug=True)
