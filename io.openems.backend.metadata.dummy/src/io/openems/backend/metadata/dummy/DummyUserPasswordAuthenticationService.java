package io.openems.backend.metadata.dummy;

import java.util.concurrent.CompletableFuture;

import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.openems.backend.authentication.api.AuthUserPasswordAuthenticationService;
import io.openems.backend.authentication.api.model.PasswordAuthenticationResult;

/**
 * Dummy implementation of {@link AuthUserPasswordAuthenticationService}.
 *
 * <p>
 * The upstream refactoring split username/password login out of
 * {@link io.openems.backend.common.metadata.Metadata} into this dedicated OSGi
 * service. Only {@code Metadata.Odoo} and {@code Authentication.OAuth2}
 * (Keycloak) provide it, so running with {@code Metadata.Dummy} alone left no
 * provider on the bus - breaking every login flow (Ui.Websocket NPE,
 * Backend2Backend.Websocket / .Rest failing to activate).
 *
 * <p>
 * This provider accepts <em>any</em> credentials and echoes the username back
 * as the user-id and token. {@link MetadataDummy#getUserByExternalId(String)}
 * then auto-creates a matching {@link io.openems.common.session.Role#ADMIN}
 * user, mirroring the Dummy metadata's existing "anything goes" behaviour. Use
 * for development and testing only.
 */
@Component(//
		name = "Metadata.Dummy.UserPasswordAuthentication", //
		service = { AuthUserPasswordAuthenticationService.class }, //
		immediate = true //
)
public class DummyUserPasswordAuthenticationService implements AuthUserPasswordAuthenticationService {

	private final Logger log = LoggerFactory.getLogger(DummyUserPasswordAuthenticationService.class);

	@Activate
	public DummyUserPasswordAuthenticationService() {
		this.log.info("Activate Metadata.Dummy.UserPasswordAuthentication");
	}

	@Deactivate
	private void deactivate() {
		this.log.info("Deactivate Metadata.Dummy.UserPasswordAuthentication");
	}

	@Override
	public CompletableFuture<PasswordAuthenticationResult> authenticateWithPassword(String username, String password) {
		return CompletableFuture
				.completedFuture(new PasswordAuthenticationResult(username, username, username));
	}

	@Override
	public CompletableFuture<PasswordAuthenticationResult> authenticateWithToken(String token) {
		return CompletableFuture.completedFuture(new PasswordAuthenticationResult(token, token, token));
	}

	@Override
	public CompletableFuture<Void> logout(String token) {
		return CompletableFuture.completedFuture(null);
	}

}
