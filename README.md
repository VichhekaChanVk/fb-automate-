# fb-automate

Local dashboard that queues and auto-publishes posts to the Facebook Pages you manage, through Meta's official Graph API.

## Features

- Import every Page you admin from one User Access Token
- Bulk-schedule a folder of videos/photos with a fixed gap between posts
- Captions from a default text, a CSV (`filename,caption`), or a `.txt` file next to each video
- Single posts: video, photo, text, or link
- Live queue view (updates every 3 s) with retry and delete
- Duplicate protection: the same video (matched by file content, not name) is never posted twice to the same Page
- Survives restarts: the queue is saved in SQLite, and missed posts resume from now with the same gaps

## Setup

```bash
pip install -r fb_scheduler/requirements.txt
python fb_scheduler/app.py
```

Open http://127.0.0.1:5000. Set `PORT` to use another port.

### Getting a token

1. On [developers.facebook.com](https://developers.facebook.com), create an app and add the **Manage everything on your Page** use case.
2. In that use case, add `pages_manage_posts`, `pages_read_engagement`, and `pages_show_list` (plus `business_management` if your Pages are in a Business portfolio).
3. In **Graph API Explorer**, select your app and **User Token**, add those permissions, click **Generate Access Token**, and tick your Pages.
4. Extend the token in the **Access Token Debugger** ("Extend Access Token"), then paste it into **Import my Pages**. Page tokens made from an extended user token do not expire.

## Notes

- The app must stay running (PC on) for scheduled posts to go out.
- Tokens are stored in `fb_scheduler/posts.db`. It is git-ignored: keep it private.
- Only post content you own or have rights to. Facebook removes and demonetizes reuploaded content.
