"""One content canonicalizer shared by websocket and history-page adapters."""
import json


def normalized_content(raw,msg_type=""):
    if isinstance(raw,str):
        try: content=json.loads(raw)
        except (ValueError,TypeError):
            return raw,[],("parsed" if msg_type in {"","text"} else "unsupported")
    else: content=raw
    if not isinstance(content,dict):
        return json.dumps(content,ensure_ascii=False),[],"unsupported"
    if isinstance(content.get("text"),str):
        return content["text"],[],"parsed"
    post=content if isinstance(content.get("content"),list) else next(
        (v for v in content.values() if isinstance(v,dict) and isinstance(v.get("content"),list)),None)
    attachments=[]
    def resource(part,kind):
        name="image_key" if part.get("image_key") else "file_key"
        if part.get(name):
            attachments.append({"kind":kind,"resource_key":part[name],
                "file_name":part.get("file_name",""),"content_status":"indexed"})
    if post is not None:
        lines=[post["title"]] if isinstance(post.get("title"),str) and post["title"] else []
        for line in post["content"]:
            if not isinstance(line,list): continue
            pieces=[]
            for part in line:
                if not isinstance(part,dict): continue
                if part.get("tag") in {"text","a"}:
                    pieces.append(str(part.get("text",part.get("href",""))))
                resource(part,"image" if part.get("image_key") else "file")
            lines.append("".join(pieces))
        return "\n".join(lines),attachments,"parsed"
    for name in ("file_key","image_key"):
        if content.get(name):
            resource({name:content[name],"file_name":content.get("file_name","")},msg_type or name[:-4])
    if attachments:
        typ=msg_type or attachments[0]["kind"]
        return "["+typ+"]"+(" "+content["file_name"] if content.get("file_name") else ""),attachments,"indexed"
    # Keep otherwise unsupported card/media content instead of replacing the
    # original with an information-losing placeholder.
    return json.dumps(content,ensure_ascii=False,sort_keys=True,separators=(",",":")),[],"unsupported"
