package main

import (
	"embed"
	"fmt"
	"github.com/labstack/echo-contrib/echoprometheus"
	"github.com/labstack/echo/v4"
	"github.com/lao-tseu-is-alive/ArrowFlightPg/pkg/db"
	"github.com/lao-tseu-is-alive/ArrowFlightPg/pkg/version"
	"github.com/lao-tseu-is-alive/go-cloud-k8s-common-libs/pkg/config"
	"github.com/lao-tseu-is-alive/go-cloud-k8s-common-libs/pkg/database"
	"github.com/lao-tseu-is-alive/go-cloud-k8s-common-libs/pkg/goHttpEcho"
	"github.com/lao-tseu-is-alive/go-cloud-k8s-common-libs/pkg/golog"
	"github.com/lao-tseu-is-alive/go-cloud-k8s-common-libs/pkg/tools"
	"github.com/prometheus/client_golang/prometheus"
	"net/http"
	"runtime"
	"strings"
)

const (
	defaultPort            = 9090
	defaultDBPort          = 5432
	defaultDBIp            = "127.0.0.1"
	defaultDBSslMode       = "prefer"
	defaultWebRootDir      = "ArrowFlightPgFront/dist/"
	defaultSecuredApi      = "/goapi/v1"
	defaultAdminUser       = "goadmin"
	defaultAdminEmail      = "goadmin@yourdomain.org"
	defaultAdminId         = 960901
	charsetUTF8            = "charset=UTF-8"
	MIMEAppJSON            = "application/json"
	MIMEHtml               = "text/html"
	MIMEHtmlCharsetUTF8    = MIMEHtml + "; " + charsetUTF8
	MIMEAppJSONCharsetUTF8 = MIMEAppJSON + "; " + charsetUTF8
)

// content holds our static web server content.
//
//go:embed ArrowFlightPgFront/dist/*
var content embed.FS

type UserLogin struct {
	PasswordHash string `json:"password_hash"`
	Username     string `json:"username"`
}

type Service struct {
	Logger golog.MyLogger
	dbConn database.DB
	server *goHttpEcho.Server
}

// login is just a trivial example to test this server
// you should use the jwt token returned from LoginUser  in github.com/lao-tseu-is-alive/go-cloud-k8s-user-group'
// and share the same secret with the above component
func (s Service) login(ctx echo.Context) error {
	s.Logger.TraceHttpRequest("login", ctx.Request())
	uLogin := new(UserLogin)
	login := ctx.FormValue("login")
	passwordHash := ctx.FormValue("hashed")
	s.Logger.Debug("login: %s, hash: %s ", login, passwordHash)
	// maybe it was not a form but a fetch data post
	if len(strings.Trim(login, " ")) < 1 {
		if err := ctx.Bind(uLogin); err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, "invalid user login or json format in request body")
		}
	} else {
		uLogin.Username = login
		uLogin.PasswordHash = passwordHash
	}
	s.Logger.Debug("About to check username: %s , password: %s", uLogin.Username, uLogin.PasswordHash)

	if s.server.Authenticator.AuthenticateUser(uLogin.Username, uLogin.PasswordHash) {
		userInfo, err := s.server.Authenticator.GetUserInfoFromLogin(login)
		if err != nil {
			errGetUInfFromLogin := fmt.Sprintf("Error getting user info from login: %v", err)
			s.Logger.Error(errGetUInfFromLogin)
			return ctx.JSON(http.StatusInternalServerError, errGetUInfFromLogin)
		}
		token, err := s.server.JwtCheck.GetTokenFromUserInfo(userInfo)
		if err != nil {
			errGetUInfFromLogin := fmt.Sprintf("Error getting jwt token from user info: %v", err)
			s.Logger.Error(errGetUInfFromLogin)
			return ctx.JSON(http.StatusInternalServerError, errGetUInfFromLogin)
		}
		// Prepare the response
		response := map[string]string{
			"token": token.String(),
		}
		s.Logger.Info("LoginUser(%s) successful login", login)
		return ctx.JSON(http.StatusOK, response)
	} else {
		return ctx.JSON(http.StatusUnauthorized, "username not found or password invalid")
	}
}

func (s Service) GetStatus(ctx echo.Context) error {
	s.Logger.TraceHttpRequest("restricted", ctx.Request())
	// get the current user from JWT TOKEN
	claims := s.server.JwtCheck.GetJwtCustomClaimsFromContext(ctx)
	currentUserId := claims.User.UserId
	s.Logger.Info("in restricted : currentUserId: %d", currentUserId)
	// you can check if the user is not active anymore and RETURN 401 Unauthorized
	//if !s.Store.IsUserActive(currentUserId) {
	//	return echo.NewHTTPError(http.StatusUnauthorized, "current calling user is not active anymore")
	//}
	return ctx.JSON(http.StatusOK, claims)
}

func (s Service) IsDBAlive() bool {
	dbVer, err := s.dbConn.GetVersion()
	if err != nil {
		return false
	}
	if len(dbVer) < 2 {
		return false
	}
	return true
}

func (s Service) checkReady(string) bool {
	// we decide what makes us ready, is a valid  connection to the database
	if !s.IsDBAlive() {
		return false
	}
	return true
}

func checkHealthy(string) bool {
	// you decide what makes you ready, may be it is the connection to the database
	//if !IsDBAlive() {
	//	return false
	//}
	return true
}

func main() {
	l, err := golog.NewLogger("zap", golog.TraceLevel, version.APP)
	if err != nil {
		panic(fmt.Sprintf("💥💥 error log.NewLogger error: %v'\n", err))
	}
	l.Info("🚀🚀 Starting App:'%s', ver:%s, from: %s", version.APP, version.VERSION, version.REPOSITORY)

	dbDsn := config.GetPgDbDsnUrlFromEnvOrPanic(defaultDBIp, defaultDBPort, tools.ToSnakeCase(version.APP), version.AppSnake, defaultDBSslMode)
	dbInstance, err := database.GetInstance("pgx", dbDsn, runtime.NumCPU(), l)
	if err != nil {
		l.Fatal("💥💥 error doing database.GetInstance(pgx ...) error: %v", err)
	}
	defer dbInstance.Close()

	dbVersion, err := dbInstance.GetVersion()
	if err != nil {
		l.Fatal("💥💥 error doing dbConn.GetVersion() error: %v", err)
	}
	l.Info("connected to db version : %s", dbVersion)

	myVersionReader := goHttpEcho.NewSimpleVersionReader(version.APP, version.VERSION, version.REPOSITORY)
	// Create a new JWT checker
	myJwt := goHttpEcho.NewJwtChecker(
		config.GetJwtSecretFromEnvOrPanic(),
		config.GetJwtIssuerFromEnvOrPanic(),
		version.APP,
		config.GetJwtContextKeyFromEnvOrPanic(),
		config.GetJwtDurationFromEnvOrPanic(60),
		l)
	// Create a new Authenticator with a simple admin user
	myAuthenticator := goHttpEcho.NewSimpleAdminAuthenticator(&goHttpEcho.UserInfo{
		UserId:     config.GetAdminIdFromEnvOrPanic(defaultAdminId),
		ExternalId: config.GetAdminExternalIdFromEnvOrPanic(9999999),
		Name:       "NewSimpleAdminAuthenticator_Admin",
		Email:      config.GetAdminEmailFromEnvOrPanic(defaultAdminEmail),
		Login:      config.GetAdminUserFromEnvOrPanic(defaultAdminUser),
		IsAdmin:    false,
	},
		config.GetAdminPasswordFromEnvOrPanic(),
		myJwt)

	server := goHttpEcho.CreateNewServerFromEnvOrFail(
		defaultPort,
		"0.0.0.0", // defaultServerIp,
		&goHttpEcho.Config{
			ListenAddress: "",
			Authenticator: myAuthenticator,
			JwtCheck:      myJwt,
			VersionReader: myVersionReader,
			Logger:        l,
			WebRootDir:    defaultWebRootDir,
			Content:       content,
			RestrictedUrl: defaultSecuredApi,
		},
	)

	e := server.GetEcho()

	// begin prometheus stuff to create a custom counter metric
	customCounter := prometheus.NewCounter( // create new counter metric. This is replacement for `prometheus.Metric` struct
		prometheus.CounterOpts{
			Name: fmt.Sprintf("%s_custom_requests_total", version.APP),
			Help: "How many HTTP requests processed, partitioned by status code and HTTP method.",
		},
	)
	if err := prometheus.Register(customCounter); err != nil { // register your new counter metric with default metrics registry
		l.Fatal("💥💥 ERROR: 'calling prometheus.Register got error: %v'\n", err)
	}
	// https://echo.labstack.com/docs/middleware/prometheus
	mwConfig := echoprometheus.MiddlewareConfig{
		AfterNext: func(c echo.Context, err error) {
			customCounter.Inc() // use our custom metric in middleware. after every request increment the counter
		},
		// does not gather metrics on routes starting with `/health`
		Skipper: func(c echo.Context) bool {
			return strings.HasPrefix(c.Path(), "/health")
		},
		Subsystem: version.APP,
	}
	e.Use(echoprometheus.NewMiddlewareWithConfig(mwConfig)) // adds middleware to gather metrics
	// end prometheus stuff to create a custom counter metric

	yourService := Service{
		Logger: l,
		dbConn: dbInstance,
		server: server,
	}

	e.GET("/metrics", echoprometheus.NewHandler()) // adds route to serve gathered metrics
	e.GET("/readiness", server.GetReadinessHandler(yourService.checkReady, "Connection to DB"))
	e.GET("/health", server.GetHealthHandler(checkHealthy, "Connection to DB"))
	// Find a way to allow Login route to be available only in dev environment
	e.POST("/login", yourService.login)
	r := server.GetRestrictedGroup()
	r.GET("/status", yourService.GetStatus)

	dbStore := db.GetStorageInstanceOrPanic("pgx", dbInstance, l)

	// now with restricted group reference you can register your secured handlers defined in OpenApi dbs.yaml
	dbService := db.Service{
		Log:              l,
		DbConn:           dbInstance,
		Store:            dbStore,
		Server:           server,
		ListDefaultLimit: 50,
	}

	db.RegisterHandlers(r, &dbService) // register all openapi declared routes

	err = server.StartServer()
	if err != nil {
		l.Fatal("💥💥 ERROR: 'calling echo.StartServer() got error: %v'\n", err)
	}

}
