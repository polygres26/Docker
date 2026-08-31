package com.nexagres.dms.http;

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
 * Serves the built {@code advisor/web} SPA (static JS/CSS/HTML) directly out of this same
 * embedded Jetty process -- closes the gap {@code advisor/web/vite.config.ts}'s own comment
 * flags ("SpaResourceHandler pattern in Omnigate, not yet ported here"). No separate nginx/
 * static-file server needed; Jetty is already a dependency of this module (see {@code
 * DmsHttpServer}), just not previously asked to serve anything but the JSON API.
 *
 * <p>Delegates real file requests (JS/CSS/images/the root {@code index.html}) to a plain Jetty
 * {@link ResourceHandler}. Any path that doesn't resolve to a real file -- a client-side route
 * like {@code /reports/42}, which only React Router knows how to render -- falls back to
 * {@code index.html} instead of a 404, standard SPA-hosting behavior (the same fallback nginx's
 * {@code try_files ... /index.html} directive gives you, see {@code docker/dms/
 * nginx.conf}).
 *
 * <p>Only reached for requests {@link DmsHttpServer} didn't claim -- see the {@code
 * HandlerList} wiring in {@link DmsHttpServer#main}, and that class's own {@code handle}
 * javadoc for why it leaves non-{@code /api/} paths unhandled instead of 404ing them itself.
 */
public class SpaResourceHandler extends AbstractHandler {

    private static final Logger log = LoggerFactory.getLogger(SpaResourceHandler.class);

    private final ResourceHandler delegate;
    private final Path indexHtml;

    public SpaResourceHandler(String webDir) {
        this.delegate = new ResourceHandler();
        delegate.setDirectoriesListed(false);
        delegate.setWelcomeFiles(new String[] {"index.html"});
        // Jetty's ResourceHandler defaults to a 302 to the literal welcome file ("/" ->
        // "/index.html") rather than serving it in place. React Router's BrowserRouter only
        // matches "/", not "/index.html" -- the client landed on a URL its own router can't
        // route, and rendered nothing. Serving index.html directly for "/" (no redirect) keeps
        // the URL the SPA actually has a route for.
        delegate.setRedirectWelcome(false);
        delegate.setResourceBase(webDir);
        this.indexHtml = Path.of(webDir, "index.html");
    }

    // Propagate this handler's Server down to the delegate *before* it starts -- a
    // ResourceHandler started without a Server attached ("No Server set for ResourceHandler")
    // can't resolve its MimeTypes and silently fails every request with 404 rather than serving
    // the SPA. AbstractHandler#setServer is called by Jetty once this handler is added to the
    // HandlerList in DmsHttpServer#main, before the container's own start() reaches doStart.
    @Override
    public void setServer(org.eclipse.jetty.server.Server server) {
        super.setServer(server);
        delegate.setServer(server);
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        delegate.start();
        log.info("Serving advisor web SPA from {} (embedded Jetty, no nginx needed)", delegate.getResourceBase());
    }

    @Override
    protected void doStop() throws Exception {
        delegate.stop();
        super.doStop();
    }

    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
            throws IOException, ServletException {
        // "/" itself: serve index.html in place rather than letting it reach the delegate
        // ResourceHandler, whose welcome-file handling 302s to the literal "/index.html" URL in
        // this Jetty version even with setRedirectWelcome(false) -- and React Router's
        // BrowserRouter has a route for "/", not for "/index.html", so that redirect landed the
        // client on a URL its own router couldn't match and rendered nothing.
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
        // Not a real static file -- treat it as an SPA client-side route and hand back
        // index.html so the SPA's own router can take over, rather than a bare 404.
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
