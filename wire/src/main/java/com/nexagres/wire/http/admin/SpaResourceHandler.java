package com.nexagres.wire.http.admin;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Serves the built {@code wire/web} SPA (static JS/CSS/HTML) directly out of this same embedded
 * Jetty admin process -- same pattern as advisor's own {@code com.nexagres.dms.http.SpaResourceHandler}
 * (that class can't be reused across modules, so this is a deliberate, small duplicate rather than
 * a new cross-module dependency).
 *
 * <p>Delegates real file requests (JS/CSS/images/the root {@code index.html}) to a plain Jetty
 * {@link ResourceHandler}. Any path that doesn't resolve to a real file -- a client-side route
 * only the SPA's own router knows how to render -- falls back to {@code index.html} instead of a
 * 404, the standard SPA-hosting fallback.
 *
 * <p>Only reached for requests {@link MetricsServer}'s own handler didn't claim -- see the
 * {@code HandlerList} wiring in {@link MetricsServer}'s constructor.
 */
public class SpaResourceHandler extends AbstractHandler {

    private static final Logger log = LoggerFactory.getLogger(SpaResourceHandler.class);

    private final ResourceHandler delegate;
    private final Path indexHtml;

    public SpaResourceHandler(String webDir) {
        this.delegate = new ResourceHandler();
        delegate.setDirectoriesListed(false);
        delegate.setWelcomeFiles(new String[] {"index.html"});
        // See dms's twin class for why redirect-welcome must stay off: a client-side router
        // only has a route for "/", not for a redirected "/index.html".
        delegate.setRedirectWelcome(false);
        delegate.setResourceBase(webDir);
        this.indexHtml = Path.of(webDir, "index.html");
    }

    // Propagate this handler's Server down to the delegate *before* it starts -- a
    // ResourceHandler started without a Server attached can't resolve its MimeTypes and silently
    // 404s every request. AbstractHandler#setServer is called once this handler is added to the
    // HandlerList, before the container's own start() reaches doStart.
    @Override
    public void setServer(org.eclipse.jetty.server.Server server) {
        super.setServer(server);
        delegate.setServer(server);
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        delegate.start();
        log.info("Serving warp admin web UI from {} (embedded Jetty, no nginx needed)", delegate.getResourceBase());
    }

    @Override
    protected void doStop() throws Exception {
        delegate.stop();
        super.doStop();
    }

    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
            throws IOException, ServletException {
        if ("/".equals(target) && Files.isRegularFile(indexHtml)) {
            response.setContentType("text/html; charset=utf-8");
            response.getOutputStream().write(Files.readAllBytes(indexHtml));
            baseRequest.setHandled(true);
            return;
        }
        delegate.handle(target, baseRequest, request, response);
        if (baseRequest.isHandled() || response.isCommitted()) {
            return;
        }
        if (!Files.isRegularFile(indexHtml)) {
            response.setStatus(404);
            baseRequest.setHandled(true);
            return;
        }
        response.setContentType("text/html; charset=utf-8");
        response.getOutputStream().write(Files.readAllBytes(indexHtml));
        baseRequest.setHandled(true);
    }
}
