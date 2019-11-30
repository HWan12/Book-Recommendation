#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Nov 30 11:35:41 2019

@author: hwan
"""

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Nov 30 11:13:59 2019

@author: hwan
"""
from pprint import pprint as pp
from flask import Flask, flash, redirect, render_template, request, url_for
from weather import query_api

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('weather.html',
                           data=[{'name':'Toronto'}, {'name':'Montreal'}, {'name':'Calgary'},
                                 {'name':'Ottawa'}, {'name':'Edmonton'}, {'name':'Mississauga'},
                                 {'name':'Winnipeg'}, {'name':'Vancouver'}, {'name':'Brampton'},
                                 {'name':'Quebec'}])

@app.route("/result" , methods=['GET', 'POST'])
def result():
    data = []
    error = None
    select = request.form.get('comp_select')
    resp = query_api(select)
    pp(resp)
    if resp:
        data.append(resp)
        if len(data) != 2:
            error = 'Bad Response from Weather API'
        return render_template('result.html',data=data,error=error)
    
if __name__=='__main__':
    app.run(debug=True)