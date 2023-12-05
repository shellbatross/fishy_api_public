from flask import Flask, request, jsonify,json
import requests
import bcrypt
import sqlite3
import os,sys
from sqlite3 import Error

from sqlalchemy import create_engine,text,exc
from sqlalchemy.orm import scoped_session, sessionmaker, declarative_base
from werkzeug.exceptions import HTTPException
"""
Hi everyone I am probably showing you all of this for my 436 project, whats up, its just the same project running in aws that i run in my local
"""
app = Flask(__name__)
sub_states = {
        "ME":{"number":"4", "reg": "North Atlantic"},"NH":{"number":"4", "reg": "North Atlantic"},"MA":{"number":"4", "reg": "North Atlantic"},"RI":{"number":"4", "reg": "North Atlantic"},"CT":{"number":"4", "reg": "North Atlantic"},
        "NY":{"number":"5","reg": "Mid-Atlantic"},"NJ":{"number":"5","reg": "Mid-Atlantic"},"DE":{"number":"5","reg": "Mid-Atlantic"},"MD":{"number":"5","reg": "Mid-Atlantic"},"VA":{"number":"5","reg": "Mid-Atlantic"},
        "NC":{"number":"6","reg":"South Atlantic"},"SC":{"number":"6","reg":"South Atlantic"},"GA":{"number":"6","reg":"South Atlantic"},"EFL":{"number":"6","reg":"South Atlantic"},
        "WFL":{"number":"7","reg":"Gulf of Mexico"},"AL":{"number":"7","reg":"Gulf of Mexico"},"MS":{"number":"7","reg":"Gulf of Mexico"},"LA":{"number":"7","reg":"Gulf of Mexico"},
        "HI":{"number":"8","reg":"West Pacific"},
        "PR":{"number":"11","reg":"US Caribbean"},
        "VISLANDS":{"number":"11","reg":"US Caribbean"}
    }

areas = {
    "1" : "Ocean fishing or extremely close to the ocean",
    "2" : "Very close to the ocean or at the shore",
    "3" : "Large amount of water lake near the ocean",
    "4":  "Quite far from ocean, big lakes or bodies of water that stream and connect to ocean",
    "5":  "Inland lakes, small bodies of water"
}

query_input_by_area={
    "1": "Inlet",
    "2": "BeachPier",
    "3": "Beach",
    "4":"Lake",
    "5":"Lake"
}

mode_map={
    "1":"Pier, Dock, Bridge",
    "2":"Beach/Bank",
    "3":"Shore",
    "4":"Headboat",
    "5":"Charter Boat",
    "7":"Private/Rental Boat"
}


#Get my aws credentials to edit db and my goog api key
with open('./config.json') as config_stuff:
    config = json.load(config_stuff)
    app.config.update(config)

engine = create_engine("mysql://"+app.config["AWS_USER"]+":"+app.config["AWS_PASSWORD"]+app.config["DB"]+".us-east-1.rds.amazonaws.com:3306/fishydb")
Session = sessionmaker(engine)
#Find db, I won't know what the full path really is when i deploy this somewhere so just gonna look for it rn lol 
"""
cur_dir = os.getcwd()
file_name = "thanks_for_all_the_fish.db"

while True:
    file_list = os.listdir(cur_dir)
    parent_dir = os.path.dirname(cur_dir)
    if file_name in file_list:
        conn = sqlite3.connect(file_name, check_same_thread = False)
        c = conn.cursor()
        break
    else:
        if cur_dir == parent_dir: #if dir is root dir then i did something evry wrong lmao wheres the db
            break
        else:
            cur_dir = parent_dir
"""
#Don't need this anymore, db is in aws also sqlite kinda sucks 

def update_user_counts(user_id,functionality):
    try:
        with Session.begin() as session:
            if functionality=="catch":
                session.execute(text("UPDATE USER SET catches=catches+1,info_sent_number=info_sent_number+1 WHERE user_id = '{id}';".format(id=user_id)))
                session.commit()
            else:
                #don't really have any other functionality except if user feels nice to send me info on their own accord
                session.execute(text("UPDATE USER SET info_sent_number=info_sent_number+1 WHERE user_id = '{id}';".format(id=user_id)))
                session.commit()
    except exc.SQLAlchemyError as e:
        msg = str(e)
        session.rollback()
        session.close()
        return msg
    except Exception as error:  
        handle_exception(error)
    else:
        session.close()
        return
    
@app.errorhandler(HTTPException)
def handle_exception(e):
    """Return JSON instead of HTML for HTTP errors."""
    # start with the correct headers and status code from the error
    response = e.get_response()
    # replace the body with JSON
    response.data = json.dumps({
        "code": e.code,
        "name": e.name,
        "description": e.description,
    })
    response.content_type = "application/json"
    return response

@app.route('/')
def check_i_work():
	return 'I am runinng!'

@app.route("/getuserinfo",methods=["GET"])
def get_user_info():
    user_id = request.args['user_id']
    try:
        with Session.begin() as session:
            res = session.execute(text("SELECT catches,info_sent_number FROM USER WHERE user_id = '{id}'".format(id=user_id)))
            session.commit()
    except exc.SQLAlchemyError as e:
        msg = str(e)
        session.rollback()
        session.close()
        return msg
    except Exception as error:  
        handle_exception(error)
    else:
        #again only happens once but easiest way to grab stuff out of SQLAlchemy result object
        catches,info_sent = 0,0
        for i in res:
            catches = i.catches
            info_sent = i.info_sent_number
        return jsonify({
            "catches":catches,
            "info_sent":info_sent
        })
    

@app.route("/gettrips",methods=["GET"])
def get_trips():
    user_id = request.args['user_id']
    trips = []
    try:
        with Session.begin() as session:
            res = session.execute(text("SELECT * FROM TRIP INNER JOIN TRIPS ON (TRIPS.trip_id = TRIP.trip_id) WHERE TRIPS.user_id = '{id}';".format(id=user_id)))
            session.commit()
    except exc.SQLAlchemyError as e:
        msg = str(e)
        session.rollback()
        session.close()
        return msg
    except Exception as error:  
        handle_exception(error)
    else:
        #return all results for this user id
            for i in res:
                curr_trip = {}
                curr_trip["trip_id"] = i.trip_id
                curr_trip["common"] = i.common
                curr_trip["address"] = i.address
                curr_trip["latitude"] = i.latitude
                curr_trip["longitude"] = i.longitude
                curr_trip["month"] = i.month
                curr_trip["sub_reg"] = i.sub_reg
                curr_trip["area_x"] = i.area_x
                curr_trip["mode_fx"]= i.mode_fx
                curr_trip["year"] = i.year
                curr_trip["wave"]= i.wave
                curr_trip["wave_numeric"]=i.wave[4:]
                trips.append(curr_trip)

    return jsonify({"results":trips})

@app.route("/deletetrip",methods=["POST"])
def delete_trip():
    incoming_req = request.json
    trip_id = incoming_req["trip_id"]
    try:
        with Session.begin() as session:
            session.execute(text("DELETE FROM TRIP WHERE trip_id = '{id}'".format(id=trip_id)))
            session.commit()
    except exc.SQLAlchemyError as e:
        msg = str(e)
        session.rollback()
        session.close()
        return msg
    except Exception as error:  
        handle_exception(error)
    else: 
        session.close()
        return "Successfully deleted trip with that trip_id"
        
@app.route("/sendfishinfo",methods = ["POST"])
def send_fish_info():
    incoming_req = request.json
    common,year,mode_fx,area_x,sub_reg,month,user_id = incoming_req["common"],incoming_req["year"], incoming_req["mode_fx"],incoming_req["area_x"],incoming_req["sub_reg"],incoming_req["month"],incoming_req["user_id"]
    wave = find_wave(month)
    wave_value = wave[4:]
    try:
        with Session.begin() as session:
            session.execute(text("INSERT INTO {wave} (common,year,mode_fx,area_x,sub_reg,wave,month) VALUES".format(wave=wave)+
                                 "('{cm}','{yr}','{md}','{area}','{sbr}','{wv}','{mm}')".format(cm=common,yr=year,md=mode_fx,area=area_x,sbr=sub_reg,wv=wave_value,mm=month)))
            session.commit()
    except exc.SQLAlchemyError as e:
        msg = str(e)
        session.rollback()
        session.close()
        return msg
    except Exception as error:  
        handle_exception(error)
    else: 
        update_user_counts(user_id,"else")
        session.close()
        return "Successfully got new info and upped user count"
    
@app.route("/catchupdate",methods=["POST"])
def catch_update():
    incoming_req = request.json
    common,year,mode_fx,area_x,sub_reg,wave,month,user_id = incoming_req["common"],incoming_req["year"], incoming_req["mode_fx"],incoming_req["area_x"],incoming_req["sub_reg"],incoming_req["wave"],incoming_req["month"],incoming_req["user_id"]
    wave_value = wave[4:]
    try:
        with Session.begin() as session:
            session.execute(text("INSERT INTO {wave} (common,year,mode_fx,area_x,sub_reg,wave,month) VALUES".format(wave=wave)+
                                 "('{cm}','{yr}','{md}','{area}','{sbr}','{wv}','{mm}')".format(cm=common,yr=year,md=mode_fx,area=area_x,sbr=sub_reg,wv=wave_value,mm=month)))
            session.commit()
    except exc.SQLAlchemyError as e:
        msg = str(e)
        session.rollback()
        session.close()
        return msg
    except Exception as error:  
        handle_exception(error)
    else: 
        update_user_counts(user_id,"catch")
        session.close()
        return "Successfully marked catch and updated users counts"

@app.route("/addtotrips",methods = ["POST"])
def add_trip():
    #I'm so sorry this is a lot but its for a good reason so I also can update if a person catched this fish to their account
    #Lol ' breaks inserting im a dummy :3
    incoming_req = request.json
    common,address,latitude,longitude,month,sub_reg,area_x,mode_fx,year,user_id= incoming_req["common"],incoming_req["address"],incoming_req["latitude"],incoming_req["longitude"],incoming_req["month"],incoming_req["sub_reg"],incoming_req["area_x"],incoming_req["mode_fx"],incoming_req["year"],incoming_req["user_id"]
    #do this to address in case it contains ': i.e Rich's island
    address = address.replace("'","''")
    wave = find_wave(month)
    try:
        with Session.begin() as session:
            session.execute(text("INSERT INTO TRIP (common,address,latitude,longitude,month,sub_reg,area_x,mode_fx,year,wave) VALUES"+
                            "('{commn}','{addrss}','{lat}','{longi}','{mnth}','{sb_reg}','{area}','{mode}','{yr}','{wv}');".format(commn=common,
                            addrss=address,lat=latitude,longi=longitude,mnth = month,sb_reg = sub_reg,area = area_x,mode = mode_fx,yr = year,wv=wave)))
            session.commit()
    except exc.SQLAlchemyError as e:
        msg = str(e)
        session.rollback()
        session.close()
        return msg
    #ELSE SERVER ERROR
   
    else:
        #Successful we need to update the user that did this insert, my thing increments by 1 so just grab max id and update table appropiately
        #SELECT fields FROM table ORDER BY id DESC LIMIT 1;
        try:
            res = session.execute(text("SELECT trip_id FROM TRIP ORDER BY trip_id DESC LIMIT 1;"))
            session.commit()
            #This only goes 1 up and thats it, but it won't allow me to just grab field
            for i in res:
                trip_id = i.trip_id
            res = session.execute(text("INSERT INTO TRIPS(user_id,trip_id) VALUES('{id}','{tripid}');".format(id=user_id,tripid=trip_id)))
            session.commit()
            session.close()
        except exc.SQLAlchemyError as e:
            msg = str(e)
            session.rollback()
            session.close()
            return msg
        except Exception as error:  
            handle_exception(error)
        else:
            session.close()
            return "Successfully added trip and updated appropiately"
    



@app.route("/gofish",methods = ["POST"])
def go_fish():
    incoming_req = request.json
    latitude,longitude,fish,month = incoming_req["latitude"],incoming_req["longitude"],incoming_req["fish"],incoming_req["month"]
    link = "https://maps.googleapis.com/maps/api/geocode/json?latlng="+latitude+","+longitude+"&key="+app.config["GOOG_API_KEY"]
    resp = requests.get(link)
    dump = resp.json()
    formatted_starting_address_list= dump["results"][0]["formatted_address"].split(" ")
    state_number,state_region,wave= "","",find_wave(month)
    
    for i in formatted_starting_address_list:
        for key,value in sub_states.items():
            if key==i:
                state_number,state_region=sub_states[key]["number"],sub_states[key]["reg"]
                break
    #now use state_number to find if any fish has been caught in this region matching the query
    try:
        with Session.begin() as session:
            res = session.execute(text("SELECT * FROM {wv} WHERE common = '{fishy}' AND  sub_reg = '{state_num}'".
                                       format(wv = wave, fishy = fish, state_num = state_number)))
            session.commit()
                
    except exc.SQLAlchemyError as e:
        msg = str(e)
        session.rollback()
        session.close()
        return msg
    #ELSE SERVER ERROR
    except Exception as error:  
         handle_exception(error)
    else:
        session.close()
        #Go through results and see if you find the best chance to find this fish is inland, or ocean
        area_map,mode_mappy = {},{}
        for i in res:
            if i.area_x not in area_map:
                area_map[i.area_x] =1
            else:
                area_map[i.area_x]+=1
            #Go through modes and see where people preferred to catch this fisht the most, warn user to be prepared for this kind of fishing
            if i.mode_fx not in mode_mappy:
                mode_mappy[i.mode_fx] = 1
            else:
                mode_mappy[i.mode_fx]+=1
        if area_map == {}:
            return "There are no results for this fish being previously caught in your area during this time of year, or with the mode selected!"
        

        #Grab whichever has the most results which implies a better chance if we fish here
        area_ans = max(area_map, key = area_map.get)
        mode_ans = max(mode_mappy, key = mode_mappy.get )
        #Okay now do a query with the description from areas
        addresses = []
        #final_resp = requests.get("https://maps.googleapis.com/maps/api/place/findplacefromtext/json?fields=formatted_address%2Cname%2Crating%2Copening_hours%2Cgeometry&input="+query_input_by_area[area_ans]+"r&inputtype=textquery&locationbias=circle%3A50000%" + latitude +"%2C"+longitude+"&key="+app.config["GOOG_API_KEY"])
        
        final_resp = requests.get("https://maps.googleapis.com/maps/api/place/nearbysearch/json?keyword="+query_input_by_area[area_ans]+"&location="+
                                  latitude +"%2C"+longitude +"&radius=500000"+"&rankby=prominence"+"&key="+app.config["GOOG_API_KEY"])
        
        
        dump = final_resp.json()
        results = dump["results"]
        for i in results:
            addresses.append((i["name"],i["geometry"]["location"]["lat"], i["geometry"]["location"]["lng"],i["vicinity"]))

        return jsonify({    
            "additional_msg": "Please rememeber this api works with very limited data of past catches in your location and this time of year!",
            "primary_msg": "There is a good chance to catch this fish in " + areas[area_ans],
            "address": addresses,
            "Preferred mode of fishing": "Most people caught it by doing " + mode_map[mode_ans] + " type of fishing",
            "mode_fx":mode_ans,
            "area_fx": area_ans,
            "sub_reg":state_number,
            "longitude":longitude,
            "latitude":latitude,
            "common":fish,
            "wave":find_wave(month)
        })
            


    """
    #Get state out of whatever location they are in
    
    #This request has a "location bias, from the current longitude and latitude within how many meters"
    return state_number + " " + state_region
    """


@app.route("/getavailablefish",methods=["POST"])
def get_vailable_fish():
    try:
        with Session.begin() as session:
            incoming_req = request.json
            month,latitude,longitude = str(incoming_req["month"]),str(incoming_req["latitude"]),str(incoming_req["longitude"])
            link = "https://maps.googleapis.com/maps/api/geocode/json?latlng="+latitude+","+longitude+"&key="+app.config["GOOG_API_KEY"]
            resp = requests.get(link)
            dump = resp.json()
            formatted_starting_address_list= dump["results"][0]["formatted_address"].split(" ")
            state_number=""
    
            for i in formatted_starting_address_list:
                for key,value in sub_states.items():
                    if key==i:
                        state_number=sub_states[key]["number"]
                        break
            wave = find_wave(month)
            res = session.execute(text("SELECT DISTINCT common FROM {wv} WHERE sub_reg = '{state_num}' AND common NOT IN  ('None','UNIDENTIFIED FISH','UNIDENTIFIED SKATE OR RAY')"
                                       .format(wv = wave, state_num = state_number)))
            session.commit()
            #find from month where this fish has been caught
            tmp = []
            for i in res:
                tmp.append(str(i.common))
            
    #SQLALCHEMY ERROR
    except exc.SQLAlchemyError as e:
        msg = str(e)
        session.rollback()
        session.close()
        return msg
    #ELSE SERVER ERROR
    except Exception as error:  
         handle_exception(error)
    else:
        session.close()
        return jsonify({
            "fishies": tmp
        })
    


@app.route("/getfish",methods=["POST"])
def get_fish():
    try:
        with Session.begin() as session:
            incoming_req = request.json
            fish,month,state= str(incoming_req["fish"]),str(incoming_req["month"]),str(incoming_req["state"])
            wave,area = find_wave(month),find_area(state)
            res = session.execute(text("SELECT * FROM {wv} WHERE common= '{fsh}'".format(wv = wave,fsh=fish)))
            session.commit()
            #find from month where this fish has been caught
            tmp = []
            for i in res:
                tmp.append(str(i))

    #SQLALCHEMY ERROR
    except exc.SQLAlchemyError as e:
        msg = str(e)
        session.rollback()
        session.close()
        return msg
    #ELSE SERVER ERROR
    except Exception as error:  
         handle_exception(error)
    else:
        session.close()
        return jsonify({
        "This many of this fish have been caught!": len(tmp)
        })

#This does not check for multiple results I just grab first result that matches both, but user_id has to be unique sooooo but I am
#running out of time on this proj like a clown 
@app.route("/login",methods = ["POST"])
def login_user():
    try:
        with Session.begin() as session:
            incoming_req = request.json
            username,password = str(incoming_req["username"]),str(incoming_req["password"])
            res = session.execute(text("SELECT user_name FROM USER WHERE user_id = '{username}' AND password = '{password}'".format(username = username, password = password)))
            ans = res.first()
            session.commit()
    #SQLALCHEMY ERROR
    except exc.SQLAlchemyError as e:
        session.rollback()
        session.close()
        msg = str(e)
        return msg
    #ELSE SERVER ERROR
    except Exception as error:  
         print(error)
         handle_exception(error)
    else:
        if not ans:
            return jsonify({"msg":" ", "name":" "})
        name = ans.user_name
        return jsonify({"msg":"User found", "name":name})
        

@app.route("/reguser",methods=["POST"])
def get_user():
    try:
        with Session.begin() as session:
            incoming_req = request.json
            user,passwrd,user_id = str(incoming_req["username"]),str(incoming_req["password"]),str(incoming_req["user_id"])
            session.execute(text(("INSERT INTO USER   (user_id,user_name,password,catches,info_sent_number) VALUES ('{user_id}','{user_input}','{password_input}',0,0)").format(user_id = user_id,user_input =user,password_input = passwrd)))
            session.commit()
        #find from month where this fish has been caught    
    #SQLALCHEMY ERROR
    except exc.SQLAlchemyError as e:
        msg = str(e)
        session.rollback()
        session.close()
        return jsonify({"msg":msg})
    #ELSE SERVER ERROR  
    except Exception as error:  
         handle_exception(error)
    else:
        session.close()
        return jsonify({
        "msg":"Successfully Registered account"
        })


    
def find_wave(month):
    waves = {"1":"wave1","2":"wave1","3":"wave2","4":"wave2","5":"wave3","6":"wave3","7":"wave4","8":"wave4","9":"wave5","10":"wave5","11":"wave6","12":"wave6"}
    return waves[month]
def find_area(state):
    return 1

    