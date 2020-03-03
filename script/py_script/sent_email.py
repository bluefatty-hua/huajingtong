# -*- coding: utf8 -*-
from smtplib import SMTP_SSL, SMTPException
from email.header import Header
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.utils import parseaddr, formataddr
from email.encoders import encode_base64
import logging
from config import EMAIL_CONF


def _format_addr(s):
    name, addr = parseaddr(s)
    return formataddr((
        Header(name, 'utf-8').encode(),
        addr.encode('utf-8') if isinstance(addr, unicode) else addr))


def send_warming_email(to_addr, subject, text):
    if not to_addr:
        logging.exception('Empty to address. Please check!')
        return
    # 输入Email地址和口令:
    from_addr = EMAIL_CONF['USER']
    code = EMAIL_CONF['CODE']
    # 输入SMTP服务器地址:
    smtp_server = EMAIL_CONF['SMTP']
    msg = MIMEText(text, 'plain', 'utf-8')
    msg['from'] = _format_addr(u'Radar_Dev<%s>' % from_addr)
    msg['to'] = _format_addr(u'<%s>' % to_addr)  # 收件人地址
    msg['Subject'] = Header(subject, 'utf-8').encode()
    try:
        server = SMTP_SSL(smtp_server, port=EMAIL_CONF['SSL_PORT'])
        # server.set_debuglevel(1)
        server.login(from_addr, code)
        server.sendmail(from_addr, [to_addr], msg.as_string())
        server.quit()
    except SMTPException:
        logging.exception('send e-mail to %s failed' % to_addr)


def send_email(to_addr, subject, qrcode_img='', text=''):
    if not to_addr:
        logging.exception('Empty to address. Please check!')
        return
    # 输入Email地址和口令:
    from_addr = EMAIL_CONF['USER']
    code = EMAIL_CONF['CODE']
    # 输入SMTP服务器地址:
    smtp_server = EMAIL_CONF['SMTP']

    msg = MIMEMultipart()
    msg['from'] = _format_addr(u'Radar_Dev<%s>' % from_addr)
    msg['to'] = _format_addr(u'<%s>' % to_addr)  # 收件人地址
    msg['Subject'] = Header(subject, 'utf-8').encode()
    if qrcode_img:
        msg.attach(MIMEText('<html><body><h1>%s</h1>' % text +
                            '<p><img src="cid:0"></p>' +
                            '</body></html>', 'html', 'utf-8'))
    else:
        msg.attach(MIMEText('<html><body><h1>%s</h1>' % text +
                            '</body></html>', 'html', 'utf-8'))
    # 设置附件的MIME和文件名，这里是png类型:
    if qrcode_img:
        mime = MIMEBase('image', 'png', filename='qrcode.png')
        # 加上必要的头信息:
        mime.add_header('Content-Disposition', 'attachment', filename='qrcode.png')
        mime.add_header('Content-ID', '<0>')
        mime.add_header('X-Attachment-Id', '0')
        # 把附件的内容读进来:
        mime.set_payload(qrcode_img)
        # 用Base64编码:
        encode_base64(mime)
        # 添加到MIMEMultipart:
        msg.attach(mime)

    try:
        server = SMTP_SSL(smtp_server, port=EMAIL_CONF['SSL_PORT'])
        server.set_debuglevel(1)
        server.login(from_addr, code)
        server.sendmail(from_addr, [to_addr], msg.as_string())
        server.quit()
    except SMTPException:
        logging.exception('send e-mail to %s failed' % to_addr)


if __name__ == '__main__':
    # send_email('test@test.com', '请使用QQ手机版扫描二维码登录', '', '')
    send_email('ruansheng@yj543.com', '请使用QQ手机版扫描二维码登录', '', '')
