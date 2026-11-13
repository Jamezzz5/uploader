"""Reddit trackers and Highlights, applied by PATCH after ad creation."""
from urllib.parse import urlsplit


CLICK = 'click_tracking_url'
VIEW = 'impression_tracking_url'
HIGHLIGHTS = 'show_highlights'
COLUMNS = (CLICK, VIEW, HIGHLIGHTS)
EVENTS = ((CLICK, 'CLICK'), (VIEW, 'VIEW'))


def urls(value):
    """Validate tracker URLs; HTML tags must be resolved before uploading."""
    if value is None or str(value).strip().lower() in ('', 'nan', 'none'):
        return []
    values = value if isinstance(value, (list, tuple)) else (
        str(value).replace('\n', '|').split('|'))
    result = []
    for item in values:
        url = str(item).strip()
        if not url:
            continue
        parsed = urlsplit(url)
        if (parsed.scheme not in ('https', 'http') or not parsed.hostname
                or any(char.isspace() for char in url) or '<' in url):
            raise ValueError('Enter a tracker URL, not an HTML tag.')
        if url not in result:
            result.append(url)
    return result


def enrollment(value):
    """Translate an explicit Highlights choice; blank keeps Reddit's value."""
    word = str(value if value is not None else '').strip().upper()
    if word in ('', 'NAN', 'NONE'):
        return None
    if word in ('FALSE', '0', 'OFF', 'NO', 'OPT_OUT'):
        return 'OPT_OUT'
    if word in ('TRUE', '1', 'ON', 'YES', 'OPT_IN'):
        return 'OPT_IN'
    raise ValueError('Choose on or off for Reddit Highlights.')


def fields(values, live=None):
    """Use the live ad to preserve omitted trackers and enhancements.
    An explicitly blank tracker clears only that event type.
    """
    body = {}
    changed = {event for column, event in EVENTS if column in values}
    if changed:
        trackers = [dict(item) for item in (live or {}).get(
            'event_trackers') or [] if item.get('type') not in changed]
        for column, event in EVENTS:
            if column in values:
                trackers.extend({'type': event, 'url': url}
                                for url in urls(values[column]))
        body['event_trackers'] = trackers
    if HIGHLIGHTS in values:
        status = enrollment(values[HIGHLIGHTS])
        if status:
            enhancements = dict((live or {}).get('enhancements') or {})
            enhancements['user_generated_content'] = {'enroll_status': status}
            body['enhancements'] = enhancements
    return body


def from_live(values):
    """Expose tracking and Highlights in the uploader's read-back columns."""
    result = {}
    for column, event in EVENTS:
        result[column] = '|'.join(dict.fromkeys(item['url'] for item in (
            values.get('event_trackers') or [])
            if item.get('type') == event and item.get('url')))
    status = ((values.get('enhancements') or {}).get(
        'user_generated_content') or {}).get('enroll_status')
    if status in ('OPT_IN', 'OPT_OUT'):
        result[HIGHLIGHTS] = 'true' if status == 'OPT_IN' else 'false'
    return result
