from flask import Flask, render_template, redirect, url_for, request, flash
from flask_sqlalchemy import SQLAlchemy
from flask_security import Security, SQLAlchemyUserDatastore, UserMixin, RoleMixin, login_required, current_user
from flask_security.utils import hash_password
from models import db, User, Comment
from datetime import datetime, timedelta
import pandas as pd
from os import environ
from google.cloud import storage
from sqlalchemy import create_engine, inspect

app = Flask(__name__)

client = storage.Client()
bucket = client.bucket(environ['GOOGLE_STORAGE_BUCKET'])

# Configuration
app.config['SQLALCHEMY_DATABASE_URI'] = f"sqlite:///{environ['DB_PATH']}"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SECRET_KEY'] = environ['SECRET_KEY']
app.config['SECURITY_PASSWORD_SALT'] = environ['SECURITY_PASSWORD_SALT']
app.config['SECURITY_REGISTERABLE'] = True
app.config['SECURITY_SEND_REGISTER_EMAIL'] = False

# Path to the metadata file
CSV_FILE_PATH = environ['CSV_FILE_PATH']

# Setup Flask-Security
user_datastore = SQLAlchemyUserDatastore(db, User, None)
security = Security(app, user_datastore)

# Initialize the database
db.init_app(app)


# Create database and tables
with app.app_context():
    db.create_all()
    if not User.query.filter_by(email=environ['DEFAULT_EMAIL']).first():
        user_datastore.create_user(
            email=environ['DEFAULT_EMAIL'], 
            password=hash_password(environ['DEFAULT_PASSWORD']))
        db.session.commit()

######################      
# Utility functions  
######################      
def generate_signed_url(bucket, blob_name, expiration_time=3600):
    """Generates a signed URL for a GCS blob."""
    blob = bucket.blob(blob_name)
    url = blob.generate_signed_url(
        expiration=timedelta(seconds=expiration_time),
        method='GET')
    return url

def sql_table_to_df(the_things):
    all_rows = the_things.query.all()
    column_names = [column.name for column in the_things.__table__.columns]
    thing_list = [[getattr(a_thing, a_column_name) for a_column_name in column_names] for a_thing in all_rows]
    df = pd.DataFrame(thing_list,columns = column_names)
    return df


  
######################      
# Routes  
######################      
  
@app.route('/')
def index():
    return render_template('index.html')

  
@app.route('/admin')
@login_required
def admin():
    if current_user.fine_role == "admin":

        all_users = sql_table_to_df(User)
        all_users = all_users[['id','email','fine_role']]
        email_addresses = all_users.email.tolist()
        
        all_comments = sql_table_to_df(Comment)
        all_comments = all_comments[['user_id','content']]
        
        comment_counts = all_comments.groupby("user_id").count().reset_index()
        comment_counts.columns = ["id","n_comments_made"]
        
        all_users = pd.merge(left=all_users,right=comment_counts,on="id",how="left")
        
        all_users.fillna(0,inplace=True)
        all_users["n_comments_made"] = all_users["n_comments_made"].astype(int)
        all_users = all_users[['id','email','fine_role','n_comments_made']]
        all_users.columns = ["user ID","Email","Role","# Comments"]

        fine_html = all_users.to_html(index=False)
      
        return render_template('admin.html',
                               fine_html=fine_html,
                               email_addresses=email_addresses)
    else:
        return render_template('index.html')


@app.route('/profile')
@login_required
def profile():
    comment_count = Comment.query.filter_by(user_id=current_user.id).count()
    return render_template('profile.html', comment_count=comment_count)


@app.route('/video_viewer', methods=['GET', 'POST'])
@login_required
def video_viewer():
    # Get the index and category parameter from the URL
    index = request.args.get('index', default=0, type=int)
    type_of_story = request.args.get('type_of_story', default='-', type=str)
    communicative_function = request.args.get('communicative_function', default='-', type=str)
    seems_political = request.args.get('seems_political', default='-', type=str)
    sensitive_topic = request.args.get('sensitive_topic', default='-', type=str)
    keyword = request.args.get('keyword', default='', type=str)

    # Read the CSV file
    df = pd.read_csv(CSV_FILE_PATH)
    
    communicative_function_options = list(map(lambda x:x.replace(","," ").split(" ")[0], df["g_communicative_function"].unique()))
    communicative_function_options = ["-"] + list(set(communicative_function_options))
    if communicative_function != "-":
        df = df[df["g_communicative_function"].map(lambda x:str(x.lower()).startswith(communicative_function.lower()))]

    type_of_story_options = list(map(lambda x:x.replace(","," ").split(" ")[0], df.g_type_of_story.unique()))
    type_of_story_options = ["-"] + list(set(type_of_story_options))
    if type_of_story != "-":
        df = df[df.g_type_of_story.map(lambda x:str(x.lower()).startswith(type_of_story.lower()))]

    sensitive_topic_options = ["-","Yes","No"]
    if sensitive_topic == "Yes":
        df = df[df.g_sensitive_topic.map(lambda x:str(x).startswith("Yes"))]
    elif sensitive_topic == "No":
        df = df[df.g_sensitive_topic.map(lambda x:str(x).startswith("No"))]

    seems_political_options = ["-","Yes","No"]
    if seems_political == "Yes":
        df = df[df.g_seems_political.map(lambda x:str(x).startswith("Yes"))]
    elif seems_political == "No":
        df = df[df.g_seems_political.map(lambda x:str(x).startswith("No"))]

    if keyword != "":
        item_id_hits = []
        for i,row in df.iterrows():
            if (any([keyword.lower() in t.lower() for t in row.values if type(t)==str])):
                item_id_hits += [row["item_id"]]
        df = df[df.item_id.isin(item_id_hits)]

    # Check if the index is within the range of the DataFrame
    if index < 0:
        index = 0
    if index >= len(df):
        index = len(df)

    comment_placeholder = "You have not made any comments to this video yet. Write your first comment here..."
    filtered_index = str(index+1) + "/" + str(len(df))

    if len(df) > 0:
        # Select the row based on the index
        video_row = df.iloc[index]
        the_filename = generate_signed_url(bucket, f"{video_row['item_id']}.mp4")

        comments_made_to_this_video = Comment.query.filter_by(
            video_id=str(video_row['item_id']),
            user_id=current_user.id
            ).count()
        if comments_made_to_this_video > 0:
            comment_placeholder = "You have already made " + str(comments_made_to_this_video) + " comment(s) to this video. If you want to add more comments, write the your comment here..."


        # Extract video details
        video = {
            'filename': the_filename,
            'title': video_row['item_id'],
            'attributes_top': [
                {'key': key, 'value': video_row[key]} for key in video_row.index if key not in ['filename', 'item_id','title'] and len(key)+len(str(video_row[key]))>100
            ],
            'attributes_bottom': [
                {'key': key, 'value': video_row[key]} for key in video_row.index if key not in ['filename', 'item_id','title'] and len(key)+len(str(video_row[key]))<=100
            ]
        }
    else:
        video = {
            'filename': None, 
            'title': "No videos",
            'attributes_top': [{'key': '', 'value': ""}],
            'attributes_bottom': [{'key': '', 'value': ""}]
        }

    # Calculate previous and next indices
    prev_index = index - 1 if index > 0 else len(df) - 1
    next_index = index + 1 if index < len(df) - 1 else 0

    # Handle comment submission
    if request.method == 'POST':
        content = request.form.get('content')
        video_id = request.form.get('video_id')
        index = request.form.get('index')
        type_of_story = request.form.get('type_of_story')
        communicative_function = request.form.get('communicative_function')
        sensitive_topic = request.form.get('sensitive_topic')
        seems_political = request.form.get('seems_political')
        keyword = request.form.get('keyword')
        #category = request.form.get('category')
        if content:
            new_comment = Comment(
                user_id=current_user.id,
                video_id=video_id,
                content=content
            )
            db.session.add(new_comment)
            db.session.commit()
            flash('Your comment has been posted!', 'success')
            return redirect(
                url_for(
                    'video_viewer',
                    index=index,
                    type_of_story=type_of_story,
                    communicative_function=communicative_function,
                    sensitive_topic=sensitive_topic,
                    seems_political=seems_political,
                    keyword=keyword
                    )
                )

    return render_template('video_viewer.html', 
        prevIndex=prev_index, 
        nextIndex=next_index, 
        thisIndex=index,
        type_of_story=type_of_story,
        type_of_story_options=type_of_story_options,
        communicative_function=communicative_function,
        communicative_function_options=communicative_function_options,
        sensitive_topic=sensitive_topic,
        sensitive_topic_options=sensitive_topic_options,
        seems_political=seems_political,
        seems_political_options=seems_political_options,
        keyword=keyword,
        filtered_index=filtered_index,
        comment_placeholder=comment_placeholder,
        video=video)


@app.route('/delete_user', methods=['POST'])
def delete_user():
    email = request.form.get('userEmail1')
    user = User.query.filter_by(email=email).first()
    if user:
        db.session.delete(user)
        db.session.commit()
        flash('User deleted successfully!', 'success')
    else:
        flash('User not found!', 'danger')
    return redirect(url_for('admin'))


@app.route('/update_user_role', methods=['POST'])
def update_user_role():
    email = request.form.get('userEmail2')
    new_role = request.form.get('newRole')
    user = User.query.filter_by(email=email).first()
    if user:
        user.fine_role = new_role
        db.session.commit()
        flash('User updated successfully!', 'success')
    else:
        flash('User not found!', 'danger')
    return redirect(url_for('admin'))

  
  

if __name__ == '__main__':
    app.run(debug=True)






