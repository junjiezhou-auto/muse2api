FROM nginx:alpine
# 官方 nginx 镜像启动时会用 envsubst 渲染 /etc/nginx/templates/*.template，
# 因此管理 Key 通过 MUSE2API_KEY 环境变量注入，而不是写进配置文件。
COPY nginx.conf.template /etc/nginx/templates/default.conf.template
